// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::handle::RestoreJobRequest;
use crate::pipeline::queue::Change;

// How many restores one turn installs.
pub(crate) const RESTORE_BATCH: usize = 32;

// A restore whose row carries a pause installs paused, to go back to what it
// was.
fn paused_restore(mut request: RestoreJobRequest, paused: bool) -> RestoreJobRequest {
    if paused && request.status != JobStatus::Paused {
        request.paused_resume_status = Some(request.status.clone());
        request.paused_resume_download_state = request.download_state;
        request.paused_resume_post_state = request.post_state;
        request.status = JobStatus::Paused;
        request.run_state = Some(crate::RunState::Paused);
    }
    request
}

impl Pipeline {
    pub(crate) async fn apply_queue_intents(&mut self) {
        self.answer_diagnostics_request();
        self.apply_queue_globals().await;
        // The walks below visit every row. They run when an intent was
        // written, a row came or went, a global gate moved, a propagation
        // hold ran out, or the last walk left something to look at again;
        // otherwise nothing they would find has changed.
        let mark = self
            .shared_state
            .queue
            .read(crate::pipeline::nzb_admission::QueueWalkMark::of);
        let now = Instant::now();
        let walk = std::mem::take(&mut self.queue_walk_due)
            || self.queue_walk_mark != Some(mark)
            || self
                .propagation_ready_at
                .values()
                .any(|(ready_at, _)| *ready_at <= now);
        self.queue_walk_mark = Some(mark);
        if !self.reconcile_nzb_admission(walk).await {
            self.settle_reprocess_pending().await;
            return;
        }
        crate::pipeline::queue::count_walk();
        // The queue window: the jobs it ranks first in dispatch order. A job
        // out of it waits as its row and its database rows alone.
        let (ranked, gone) = self.shared_state.queue.read(|inner| {
            (
                self.queue_window_ranks(inner),
                self.forget_gone_parked(inner),
            )
        });
        self.drop_parked(gone);
        // Installing a job is its add, whether it comes from intake or from a
        // restart, under any pause and with every slot taken; only a shutdown
        // installs nothing. Admission decides only whether its articles go
        // out. A restore whose row carries a pause installs parked.
        // A restart's restores, and the jobs entering the window, install a
        // bounded batch a turn: the jobs holding a slot first, wherever they
        // stand, then the rest in queue order, so the jobs that download
        // start while the rest install. A restore out of the window is not
        // installed: its job is read back from the database once it enters.
        let (tickets, entering, parking) = self.shared_state.queue.write(|inner| {
            if inner.globals.shutting_down {
                return Change::unchanged((Vec::new(), Vec::new(), Vec::new()));
            }
            let enters = |row: &crate::pipeline::queue::JobRow| {
                (row.intents.restore.is_some() || self.window_parked(row.id))
                    && !self.window_unreadable.contains(&row.id)
                    && self.in_queue_window(&ranked, row)
            };
            let mut holders: Vec<usize> = self
                .nzb_admitted
                .iter()
                .filter_map(|id| inner.job_position(*id))
                .filter(|position| enters(&inner.jobs[*position]))
                .collect();
            holders.sort_unstable();
            let order: Vec<usize> = holders
                .into_iter()
                .chain((0..inner.jobs.len()).filter(|position| {
                    let row = &inner.jobs[*position];
                    !self.nzb_admitted.contains(&row.id) && enters(row)
                }))
                .take(RESTORE_BATCH)
                .collect();
            let mut tickets = Vec::new();
            let mut entering = Vec::new();
            for position in order {
                let row = &mut inner.jobs[position];
                let paused = row.intents.paused == Some(true);
                match row.intents.restore.take() {
                    Some(request) => tickets.push(paused_restore(request, paused)),
                    None => entering.push((row.id, paused)),
                }
            }
            let mut parking = Vec::new();
            for row in &mut inner.jobs {
                if row.intents.restore.is_some() && !self.in_queue_window(&ranked, row) {
                    let request = row.intents.restore.take().expect("restore ticket");
                    let articles = match &request.restart {
                        Some(read) => read
                            .summaries
                            .0
                            .iter()
                            .map(|file| u64::from(file.remaining))
                            .sum(),
                        None => request
                            .spec
                            .files
                            .iter()
                            .map(|file| file.segments.len() as u64)
                            .sum(),
                    };
                    row.report.restoring = false;
                    parking.push((row.id, articles));
                }
            }
            if parking.is_empty() {
                Change::unchanged((tickets, entering, parking))
            } else {
                Change::changed((tickets, entering, parking))
            }
        });
        // A full batch may have left more behind; the next turn looks.
        self.restores_waiting = tickets.len() + entering.len() == RESTORE_BATCH;
        self.queue_walk_due |= self.restores_waiting;
        if !parking.is_empty() {
            for (id, articles) in &parking {
                self.park_window_job(*id, *articles);
            }
            self.update_queue_metrics();
            self.restate_wait_reasons(parking.iter().map(|(id, _)| *id));
        }
        if !tickets.is_empty() {
            self.restore_pass
                .get_or_insert_with(|| (Instant::now(), 0))
                .1 += tickets.len();
        }
        let loads = self
            .load_parked_jobs(entering.iter().map(|(id, _)| *id).collect())
            .await;
        let mut restores: Vec<(RestoreJobRequest, bool)> = tickets
            .into_iter()
            .map(|request| (request, false))
            .collect();
        for ((id, paused), (_, loaded)) in entering.into_iter().zip(loads) {
            if let Some(request) = self.take_parked_load(id, loaded) {
                restores.push((paused_restore(request, paused), true));
            }
        }
        if !restores.is_empty() {
            self.download_dispatch_wake = true;
        }
        for (request, entered) in restores {
            let id = request.job_id;
            if let Err(error) = self.install_restored_job(request).await {
                error!(job_id = id.0, %error, "failed to install restore ticket");
            }
            if entered {
                self.settle_window_entry(id);
            } else if let Some(values) = self.active_runtime_values(id) {
                self.window_stored.insert(id, values);
            }
            // An installed job is a new candidate for a slot.
            self.queue_walk_due = true;
            self.report_job(id);
            self.shared_state.queue.write(|inner| {
                if let Some(row) = inner.job_mut(id) {
                    row.report.restoring = false;
                }
                Change::changed(())
            });
        }
        if !self.restores_waiting
            && let Some((started, installed)) = self.restore_pass.take()
        {
            info!(
                installed,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "installed every job a restart restored"
            );
        }
        let mut deferred = false;
        let admissions = self.shared_state.queue.write(|inner| {
            if inner.globals.shutting_down {
                return Change::unchanged(Vec::new());
            }
            Change::unchanged(
                inner
                    .jobs
                    .iter_mut()
                    .filter(|row| {
                        // A history retry can reuse the ID while the previous
                        // graph run is still draining its terminal workers.
                        // The next turn looks again.
                        let waits = row.admission.is_some() && self.awaits_job_articles(row.id);
                        deferred |= waits;
                        !waits
                    })
                    .filter_map(|row| row.admission.take().map(|job| (row.id, job)))
                    .collect::<Vec<_>>(),
            )
        });
        self.queue_walk_due |= deferred || !admissions.is_empty();
        let mut parked_adds = Vec::new();
        for (id, job) in admissions {
            let reprocess = job
                .restart
                .as_ref()
                .is_some_and(|restart| restart.reprocess);
            // The arrival's scripts run for every add as it is taken in,
            // whether the job is installed or waits out of the window.
            if !reprocess {
                self.raise_added_script_event_for_add(id, &job);
            }
            // A fresh add out of the window is persisted already; it waits
            // as its row until it enters.
            let installs = job.restart.is_some()
                || self.shared_state.queue.read(|inner| {
                    inner
                        .job(id)
                        .is_none_or(|row| self.in_queue_window(&ranked, row))
                });
            if !installs {
                let mut spec = job.spec;
                let summaries = spec.drop_article_lists();
                let articles = summaries
                    .0
                    .iter()
                    .map(|file| u64::from(file.remaining))
                    .sum();
                self.park_window_job(id, articles);
                parked_adds.push(id);
                continue;
            }
            self.install_prepared_add(id, job);
            self.nzb_reprocess_pending.remove(&id);
            if reprocess && self.nzb_admitted.contains(&id) {
                self.check_job_completion(id).await;
            } else if reprocess {
                self.nzb_reprocess_pending.insert(id);
            }
        }
        if !parked_adds.is_empty() {
            self.update_queue_metrics();
            self.restate_wait_reasons(parked_adds.into_iter());
        }
        let (order, pending) = self.shared_state.queue.read(|inner| {
            (
                inner.jobs.iter().map(|row| row.id).collect::<Vec<_>>(),
                inner
                    .jobs
                    .iter()
                    .filter(|row| row.intents.seq != row.report.seen_intent_seq)
                    .map(|row| {
                        (
                            row.id,
                            row.intents.clone(),
                            row.intents.pause_seq > row.report.seen_intent_seq,
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        });
        let installed_order: Vec<_> = order
            .iter()
            .copied()
            .filter(|id| self.jobs.contains_key(id))
            .collect();
        if installed_order != self.job_order {
            self.job_order = installed_order;
        }
        if order != self.stored_queue_order {
            self.persist_queue_order(&order).await;
            self.stored_queue_order = order;
        }
        use crate::jobs::handle::CancellationOrigin;
        for origin in [
            CancellationOrigin::User,
            CancellationOrigin::UserDuplicate,
            CancellationOrigin::UserKeepingFiles,
            CancellationOrigin::SemanticBad,
            CancellationOrigin::SemanticSuperseded,
        ] {
            let ids: BTreeSet<_> = pending
                .iter()
                .filter_map(|(id, intents, _)| {
                    (intents.cancelled == Some(origin)
                        && (self.jobs.contains_key(id) || self.window_unreadable.contains(id)))
                    .then_some(*id)
                })
                .collect();
            for id in &ids {
                self.shared_state.cancel_active_job_work(*id);
            }
            if !ids.is_empty() {
                self.start_cancel_jobs(ids, origin);
            }
        }
        for (job_id, intents, apply_pause) in pending {
            if intents.cancelled.is_some() || !self.jobs.contains_key(&job_id) {
                continue;
            }
            if intents.cancel_post_processing {
                if let Some(sender) = self.terminal_post_processing_cancellations.get(&job_id) {
                    sender.send_replace(true);
                }
                self.terminal_post_processing_executor.cancel_job(job_id.0);
            }
            // A pause or resume the job's state no longer allows is refused
            // here: the user's flag is withdrawn so the view never shows an
            // intent the pipeline did not act on.
            let mut refused = false;
            if let Some(paused) = intents.paused.filter(|_| apply_pause) {
                // A resume waits for its slot; the admission pass states the
                // wait, and the walk after the slot is taken applies it.
                if !paused && !self.nzb_admitted.contains(&job_id) {
                    continue;
                }
                if paused
                    && let Err(error) = self
                        .db_blocking(move |db| db.set_nzb_pause_release(job_id, true))
                        .await
                {
                    error!(job_id = job_id.0, %error, "failed to persist NZB pause release");
                    self.queue_walk_due = true;
                    continue;
                }
                let result = if paused {
                    self.pause_job_runtime(job_id)
                } else {
                    self.resume_restored_job(job_id).await
                };
                match result {
                    Ok(()) => {
                        self.report_job(job_id);
                        if paused {
                            self.nzb_pause_releases.insert(job_id);
                            self.emit_download_finished_if_active(job_id);
                        } else {
                            // Keep the interrupted-pause marker until the resumed
                            // lifecycle has committed. A crash in between restores
                            // a paused job instead of leaking an occupied slot.
                            let result = match self.db.flush_write_queue().await {
                                Ok(()) => {
                                    self.db_blocking(move |db| {
                                        db.set_nzb_pause_release(job_id, false)
                                    })
                                    .await
                                }
                                Err(error) => Err(error),
                            };
                            match result {
                                Ok(()) => {
                                    self.nzb_pause_releases.remove(&job_id);
                                }
                                Err(error) => {
                                    error!(job_id = job_id.0, %error, "failed to settle NZB resume")
                                }
                            }
                        }
                        self.emit_job_event(if paused {
                            PipelineEvent::JobPaused { job_id }
                        } else {
                            PipelineEvent::JobResumed { job_id }
                        });
                    }
                    Err(error) => {
                        warn!(job_id = job_id.0, paused, %error, "pipeline refused the job's pause intent");
                        if paused
                            && let Err(error) = self
                                .db_blocking(move |db| db.set_nzb_pause_release(job_id, false))
                                .await
                        {
                            error!(job_id = job_id.0, %error, "failed to withdraw NZB pause release");
                        }
                        if !paused
                            && self.jobs.get(&job_id).is_some_and(|state| {
                                state.run_state == crate::jobs::model::RunState::Paused
                            })
                        {
                            // A refused resume must return its newly acquired
                            // slot once any recovery work has quiesced.
                            if let Err(error) = self
                                .db_blocking(move |db| db.set_nzb_pause_release(job_id, true))
                                .await
                            {
                                error!(job_id = job_id.0, %error, "failed to persist refused NZB resume");
                                self.queue_walk_due = true;
                                continue;
                            }
                            self.nzb_pause_releases.insert(job_id);
                        }
                        refused = true;
                    }
                }
                // Work deferred behind the pause intent goes once the job
                // is live, whether the intent was a resume or was refused.
                if self
                    .jobs
                    .get(&job_id)
                    .is_some_and(|state| state.run_state != crate::jobs::model::RunState::Paused)
                    && self.deferred_moves.contains(&job_id)
                {
                    self.schedule_job_completion_check(job_id);
                }
            }
            self.apply_pending_job_update(job_id);
            self.shared_state.queue.write(|inner| {
                if let Some(row) = inner.job_mut(job_id) {
                    row.report.seen_intent_seq = intents.seq;
                    if refused && row.intents.pause_seq == intents.pause_seq {
                        row.intents.paused = None;
                    }
                    if row.intents.seq == intents.seq {
                        row.intents.update = None;
                        row.intents.cancel_post_processing = false;
                    }
                }
                Change::changed(())
            });
            self.report_job(job_id);
        }
        self.settle_reprocess_pending().await;
        let renames = self.shared_state.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .filter(|row| row.intents.cancelled.is_none())
                .filter_map(|row| row.intents.rename_to.clone().map(|name| (row.id, name)))
                .collect::<Vec<_>>()
        });
        for (id, name) in renames {
            if self.rename_working_dir_when_quiet(id, &name).await {
                self.shared_state.queue.write(|inner| {
                    if let Some(row) = inner.job_mut(id)
                        && row.intents.rename_to.as_ref() == Some(&name)
                    {
                        row.intents.rename_to = None;
                    }
                    Change::changed(())
                });
                self.report_job(id);
            } else {
                // Renamed once the job is quiet; the next turn looks again.
                self.queue_walk_due = true;
            }
        }
        self.evict_outside_window(&ranked);
    }

    // Restate the wait reason the rows of the jobs named store.
    fn restate_wait_reasons(&self, ids: impl Iterator<Item = JobId>) {
        self.shared_state.queue.write(|inner| {
            for id in ids {
                if let Some(row) = inner.job_mut(id) {
                    let reason = self.stored_wait_reason(row);
                    row.report.job.download_wait_reason = reason.map(str::to_owned);
                }
            }
            Change::changed(())
        });
    }

    // Stores the queue's order by writing a new position only for the rows
    // whose stored one no longer sorts where the row stands. The write goes
    // through the ordered writer queue, which the shutdown drain flushes
    // within its bound and warns about if it cannot.
    async fn persist_queue_order(&mut self, order: &[JobId]) {
        if self
            .queue_positions_lost
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.queue_positions = None;
        }
        let persisted = match self.queue_positions.take() {
            Some(persisted) => persisted,
            None => match self
                .db_blocking(|db| db.load_active_job_queue_positions())
                .await
            {
                Ok(persisted) => persisted,
                Err(error) => {
                    error!(%error, "failed to read stored queue order");
                    HashMap::new()
                }
            },
        };
        let writes = queue_position_writes(order, &persisted);
        let mut positions: HashMap<JobId, i64> = order
            .iter()
            .filter_map(|id| persisted.get(id).map(|position| (*id, *position)))
            .collect();
        positions.extend(writes.iter().copied());
        if writes.is_empty() {
            self.queue_positions = Some(positions);
            return;
        }
        // A write that does not commit leaves the database holding other
        // positions than these, so the next order change reads them back.
        let lost = Arc::clone(&self.queue_positions_lost);
        match self
            .db
            .try_queue_write("update_active_job_queue_positions", move |db| {
                db.update_active_job_queue_positions(&writes)
                    .inspect_err(|_| lost.store(true, std::sync::atomic::Ordering::Release))
            }) {
            Ok(()) => self.queue_positions = Some(positions),
            // Read back from the database before the next order is written.
            Err(error) => error!(%error, "failed to persist queue order"),
        }
    }

    // A reprocessed job is judged once it holds a slot and runs.
    async fn settle_reprocess_pending(&mut self) {
        let reprocess: Vec<_> = self
            .nzb_reprocess_pending
            .iter()
            .copied()
            .filter(|id| {
                self.nzb_admitted.contains(id)
                    && self.jobs.get(id).is_some_and(|state| {
                        state.run_state == crate::jobs::model::RunState::Active
                    })
            })
            .collect();
        for id in reprocess {
            self.nzb_reprocess_pending.remove(&id);
            self.check_job_completion(id).await;
        }
        self.nzb_reprocess_pending
            .retain(|id| self.jobs.contains_key(id));
    }

    pub(crate) fn apply_pending_job_update(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let update = self.shared_state.queue.write(|inner| {
            let Some(row) = inner.job_mut(job_id) else {
                return Change::unchanged(None);
            };
            let Some(update) = row.intents.update.take() else {
                return Change::unchanged(None);
            };
            update.apply_to_spec(&mut state.spec);
            row.report.job.category = state.spec.category.clone();
            row.report.job.password = state.spec.password.clone();
            row.report.job.name = state.spec.name.clone();
            row.report.job.metadata = state.spec.metadata.clone();
            Change::changed(Some(update))
        });
        if let Some(update) = update
            && let Err(error) =
                self.db
                    .try_queue_job_write(job_id, "update_active_job", move |db| {
                        db.update_active_job(job_id, &update)
                    })
        {
            error!(%error, "failed to persist job intent");
        }
    }

    async fn apply_queue_globals(&mut self) {
        let pending = self
            .shared_state
            .queue
            .write(|inner| Change::unchanged(inner.nntp.take()));
        if let Some((generation, client, connections)) = pending {
            self.activate_nntp(client, connections, generation);
        }
        let iops = self
            .shared_state
            .queue
            .read(|inner| inner.globals.random_read_iops);
        if let Some(iops) = iops.filter(|iops| self.applied_random_read_iops != Some(*iops)) {
            self.applied_random_read_iops = Some(iops);
            self.tuner.set_random_read_iops(iops);
        }
        let write_mbps = self
            .shared_state
            .queue
            .read(|inner| inner.globals.sequential_write_mbps);
        if let Some(write_mbps) =
            write_mbps.filter(|mbps| self.applied_sequential_write_mbps != Some(*mbps))
        {
            self.applied_sequential_write_mbps = Some(write_mbps);
            self.tuner.set_sequential_write_mbps(write_mbps);
        }
        let globals = self.shared_state.queue.read(|inner| inner.globals.clone());
        if globals.paused != self.global_paused || globals.scheduled_pause != self.scheduled_pause {
            self.download_dispatch_stall = None;
            self.global_paused = globals.paused;
            self.scheduled_pause = globals.scheduled_pause;
            if globals.paused {
                self.download_restart_durable_lead_retry_after.clear();
            }
            let paused = globals.paused;
            if !globals.scheduled_pause
                && let Err(error) = self.db.try_queue_write("global_paused", move |db| {
                    db.set_setting("global_paused", if paused { "true" } else { "false" })
                })
            {
                error!(%error, "failed to persist global pause");
            }
            self.emit_job_event(if self.shared_state.is_paused() {
                PipelineEvent::GlobalPaused
            } else {
                PipelineEvent::GlobalResumed
            });
            self.hold_extraction_clocks();
        }
        if globals.speed_test_paused != self.applied_speed_test_pause {
            self.applied_speed_test_pause = globals.speed_test_paused;
            self.download_restart_durable_lead_retry_after.clear();
            self.hold_extraction_clocks();
        }
        if self.configured_rate_limit != globals.speed_limit
            || self.scheduled_rate_limit != globals.scheduled_speed_limit
        {
            self.configured_rate_limit = globals.speed_limit;
            self.scheduled_rate_limit = globals.scheduled_speed_limit;
            self.rate_limiter
                .set_rate(globals.scheduled_speed_limit.unwrap_or(globals.speed_limit));
        }
        if self.propagation_delay != globals.propagation_delay {
            self.propagation_delay = globals.propagation_delay;
            let held: Vec<_> = self
                .propagation_ready_at
                .drain()
                .map(|(id, _)| id)
                .collect();
            for job_id in held {
                if self.propagation_hold_until(job_id).is_none() {
                    self.emit_job_event(PipelineEvent::PhaseProgressUpdated { job_id });
                }
            }
        }
        if let Some(profile) = globals.hardware_profile
            && profile != self.configured_hardware_profile
        {
            self.set_configured_hardware_profile(profile);
        }
        if globals.scheduled_hardware_profile != self.scheduled_hardware_profile {
            self.set_scheduled_hardware_profile(globals.scheduled_hardware_profile);
        }
        if let Some(paths) = globals.runtime_paths
            && (self.intermediate_dir != paths.intermediate
                || self.complete_dir != paths.complete
                || self.nzb_dir != paths.data.join(".weaver-nzbs"))
        {
            use crate::operations::StorageRoot;
            self.storage_capacity
                .retarget(StorageRoot::Data, paths.data.clone());
            self.storage_capacity
                .retarget(StorageRoot::Intermediate, paths.intermediate.clone());
            self.storage_capacity
                .retarget(StorageRoot::Complete, paths.complete.clone());
            self.intermediate_dir = paths.intermediate;
            self.complete_dir = paths.complete;
            self.nzb_dir = paths.data.join(".weaver-nzbs");
        }
        self.publish_download_block();
        let paused = globals.post_processing_paused;
        if paused == self.terminal_post_processing_executor.is_paused() {
            return;
        }
        if paused {
            self.terminal_post_processing_executor.pause();
            return;
        }
        self.terminal_post_processing_executor.resume();
        let deferred_moves: Vec<_> = self.deferred_moves.drain().collect();
        for job_id in deferred_moves {
            if self.shared_state.queue.read(|inner| {
                inner
                    .jobs
                    .iter()
                    .any(|row| row.id == job_id && row.intents.paused == Some(true))
            }) {
                self.deferred_moves.insert(job_id);
                continue;
            }
            if self.jobs.get(&job_id).is_some_and(|state| {
                !matches!(
                    state.status,
                    JobStatus::Paused
                        | JobStatus::Complete
                        | JobStatus::Partial
                        | JobStatus::Failed { .. }
                )
            }) && let Err(error) = self.start_move_to_complete(job_id).await
            {
                self.fail_job(job_id, error);
            }
        }
        // The pause held every graph job's repair and extraction slots. Each
        // live job asks for them again, and its completion gate looks again.
        let jobs: Vec<_> = self.graph_jobs.keys().copied().collect();
        for job_id in jobs {
            if self.shared_state.queue.read(|inner| {
                inner.job(job_id).is_some_and(|row| {
                    row.intents.paused == Some(true) || row.intents.cancelled.is_some()
                })
            }) {
                continue;
            }
            self.apply_pending_job_update(job_id);
            if !self.jobs.contains_key(&job_id) {
                continue;
            }
            self.resume_graph_work(job_id);
        }
    }
}

// Spacing between positions given out fresh, so later moves usually find
// room between two neighbours without renumbering.
const QUEUE_POSITION_STRIDE: i64 = 1024;

// The position writes that make stored positions ascend along `order`. The
// longest run of rows whose stored positions already ascend keeps them; each
// other row gets a position between its kept neighbours. Only when two kept
// neighbours leave no room does every row get a fresh, spaced position.
pub(crate) fn queue_position_writes(
    order: &[JobId],
    persisted: &HashMap<JobId, i64>,
) -> Vec<(JobId, i64)> {
    let keys: Vec<Option<i64>> = order.iter().map(|id| persisted.get(id).copied()).collect();
    // Longest strictly ascending run of stored positions, by patience
    // sorting: `tails[k]` is the index ending the best run of length k + 1.
    let mut tails: Vec<usize> = Vec::new();
    let mut previous: Vec<Option<usize>> = vec![None; order.len()];
    for (index, key) in keys.iter().enumerate() {
        let Some(key) = *key else { continue };
        let at = tails.partition_point(|&tail| keys[tail].expect("kept rows have positions") < key);
        previous[index] = at.checked_sub(1).map(|before| tails[before]);
        if at == tails.len() {
            tails.push(index);
        } else {
            tails[at] = index;
        }
    }
    let mut kept = vec![false; order.len()];
    let mut cursor = tails.last().copied();
    while let Some(index) = cursor {
        kept[index] = true;
        cursor = previous[index];
    }
    let mut writes = Vec::new();
    let mut index = 0;
    let mut low: Option<i64> = None;
    while index < order.len() {
        if kept[index] {
            low = keys[index];
            index += 1;
            continue;
        }
        let end = (index..order.len())
            .find(|&at| kept[at])
            .unwrap_or(order.len());
        let high = keys.get(end).copied().flatten();
        let count = (end - index) as i64;
        let fresh: Vec<i64> = match (low, high) {
            (None, None) => (0..count)
                .map(|step| step * QUEUE_POSITION_STRIDE)
                .collect(),
            (Some(low), None) => (1..=count)
                .map(|step| low + step * QUEUE_POSITION_STRIDE)
                .collect(),
            (None, Some(high)) => (0..count)
                .map(|step| high - (count - step) * QUEUE_POSITION_STRIDE)
                .collect(),
            (Some(low), Some(high)) if high - low > count => {
                let gap = (high - low) / (count + 1);
                (1..=count).map(|step| low + step * gap).collect()
            }
            (Some(_), Some(_)) => {
                return order
                    .iter()
                    .enumerate()
                    .map(|(at, id)| (*id, at as i64 * QUEUE_POSITION_STRIDE))
                    .filter(|(id, position)| persisted.get(id) != Some(position))
                    .collect();
            }
        };
        writes.extend(order[index..end].iter().copied().zip(fresh));
        index = end;
    }
    writes
}

#[cfg(test)]
mod position_tests {
    use super::*;

    fn ids(range: std::ops::Range<u64>) -> Vec<JobId> {
        range.map(JobId).collect()
    }

    // Applies `writes` and checks the stored positions ascend along `order`.
    fn assert_sorted(
        order: &[JobId],
        persisted: &mut HashMap<JobId, i64>,
        writes: &[(JobId, i64)],
    ) {
        persisted.extend(writes.iter().copied());
        let stored: Vec<i64> = order.iter().map(|id| persisted[id]).collect();
        assert!(
            stored.windows(2).all(|pair| pair[0] < pair[1]),
            "{stored:?}"
        );
    }

    #[test]
    fn a_queue_written_once_is_not_written_again() {
        let order = ids(0..10_000);
        let mut persisted = HashMap::new();
        let writes = queue_position_writes(&order, &persisted);
        assert_eq!(writes.len(), order.len());
        assert_sorted(&order, &mut persisted, &writes);
        assert!(queue_position_writes(&order, &persisted).is_empty());
    }

    #[test]
    fn moving_one_job_writes_one_row_once_the_positions_are_spaced() {
        let mut order = ids(0..10_000);
        let mut persisted = HashMap::new();
        let writes = queue_position_writes(&order, &persisted);
        assert_sorted(&order, &mut persisted, &writes);

        // To the top, to the bottom, and into the middle.
        for (from, to) in [(9_999, 0), (0, 9_999), (5_000, 1_234)] {
            let id = order.remove(from);
            order.insert(to, id);
            let writes = queue_position_writes(&order, &persisted);
            assert_eq!(writes, vec![(id, writes[0].1)]);
            assert_sorted(&order, &mut persisted, &writes);
        }

        // Removing rows and appending one writes only the new row.
        order.retain(|id| id.0 % 3 != 0);
        order.push(JobId(20_000));
        let writes = queue_position_writes(&order, &persisted);
        assert_eq!(writes.len(), 1);
        assert_sorted(&order, &mut persisted, &writes);
    }

    #[test]
    fn dense_positions_with_no_room_are_respaced_once() {
        let mut order = ids(0..100);
        let mut persisted: HashMap<JobId, i64> =
            order.iter().map(|id| (*id, id.0 as i64)).collect();
        let id = order.remove(50);
        order.insert(10, id);
        let writes = queue_position_writes(&order, &persisted);
        assert_sorted(&order, &mut persisted, &writes);
        let id = order.remove(70);
        order.insert(20, id);
        let writes = queue_position_writes(&order, &persisted);
        assert_eq!(writes.len(), 1);
        assert_sorted(&order, &mut persisted, &writes);
    }
}
