// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::queue::Change;

impl Pipeline {
    pub(crate) async fn apply_queue_intents(&mut self) {
        self.answer_diagnostics_request();
        self.apply_queue_globals().await;
        self.reconcile_nzb_admission().await;
        // Installing a job is its add, whether it comes from intake or from a
        // restart, under any pause and with every slot taken; only a shutdown
        // installs nothing. Admission decides only whether its articles go
        // out. A restore whose row carries a pause installs parked.
        let restores = self.shared_state.queue.write(|inner| {
            if inner.globals.shutting_down {
                return Change::unchanged(Vec::new());
            }
            Change::unchanged(
                inner
                    .jobs
                    .iter_mut()
                    .filter_map(|row| {
                        let paused = row.intents.paused == Some(true);
                        row.intents.restore.take().map(|mut request| {
                            if paused && request.status != JobStatus::Paused {
                                request.paused_resume_status = Some(request.status.clone());
                                request.paused_resume_download_state = request.download_state;
                                request.paused_resume_post_state = request.post_state;
                                request.status = JobStatus::Paused;
                                request.run_state = Some(crate::RunState::Paused);
                            }
                            request
                        })
                    })
                    .collect::<Vec<_>>(),
            )
        });
        for request in restores {
            let id = request.job_id;
            if let Err(error) = self.install_restored_job(request).await {
                error!(job_id = id.0, %error, "failed to install restore ticket");
            }
            self.report_job(id);
            self.shared_state.queue.write(|inner| {
                if let Some(row) = inner.jobs.iter_mut().find(|row| row.id == id) {
                    row.report.restoring = false;
                }
                Change::changed(())
            });
        }
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
                        !self.awaits_job_articles(row.id)
                    })
                    .filter_map(|row| row.admission.take().map(|job| (row.id, job)))
                    .collect::<Vec<_>>(),
            )
        });
        for (id, job) in admissions {
            let reprocess = job
                .restart
                .as_ref()
                .is_some_and(|restart| restart.reprocess);
            self.install_prepared_add(id, job);
            self.nzb_reprocess_pending.remove(&id);
            if reprocess && self.nzb_admitted.contains(&id) {
                self.check_job_completion(id).await;
            } else if reprocess {
                self.nzb_reprocess_pending.insert(id);
            }
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
            .into_iter()
            .filter(|id| self.jobs.contains_key(id))
            .collect();
        if installed_order != self.job_order {
            self.job_order = installed_order;
            let positions: Vec<_> = self.shared_state.queue.read(|inner| {
                inner
                    .jobs
                    .iter()
                    .enumerate()
                    .map(|(index, row)| (row.id, index as i64))
                    .collect()
            });
            if let Err(error) = self
                .db
                .try_queue_write("update_active_job_queue_positions", move |db| {
                    db.update_active_job_queue_positions(&positions)
                })
            {
                error!(%error, "failed to persist queue order");
            }
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
                    (intents.cancelled == Some(origin) && self.jobs.contains_key(id)).then_some(*id)
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
                if !paused && !self.nzb_admitted.contains(&job_id) {
                    self.report_job(job_id);
                    continue;
                }
                if paused
                    && let Err(error) = self
                        .db_blocking(move |db| db.set_nzb_pause_release(job_id, true))
                        .await
                {
                    error!(job_id = job_id.0, %error, "failed to persist NZB pause release");
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
                if let Some(row) = inner.jobs.iter_mut().find(|row| row.id == job_id) {
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
                    if let Some(row) = inner.jobs.iter_mut().find(|row| row.id == id)
                        && row.intents.rename_to.as_ref() == Some(&name)
                    {
                        row.intents.rename_to = None;
                    }
                    Change::changed(())
                });
                self.report_job(id);
            }
        }
    }

    pub(crate) fn apply_pending_job_update(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let update = self.shared_state.queue.write(|inner| {
            let Some(row) = inner.jobs.iter_mut().find(|row| row.id == job_id) else {
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
        }
        if globals.speed_test_paused != self.applied_speed_test_pause {
            self.applied_speed_test_pause = globals.speed_test_paused;
            self.download_restart_durable_lead_retry_after.clear();
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
                inner.jobs.iter().any(|row| {
                    row.id == job_id
                        && (row.intents.paused == Some(true) || row.intents.cancelled.is_some())
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
