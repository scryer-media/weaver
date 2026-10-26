// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl Pipeline {
    pub(crate) fn purge_terminal_job_runtime(&mut self, job_id: JobId) {
        self.queue_scripts_completed.remove(&job_id);
        self.added_script_holds.remove(&job_id);
        self.jobs.remove(&job_id);
        self.forget_graph_repair(job_id);
        if let Some(graph) = self.graph_jobs.get(&job_id) {
            graph.request_stop();
            if !graph.awaits_mail() {
                self.graph_jobs.remove(&job_id);
            }
        }
        self.pipeline_failure_stages.remove(&job_id);
        crate::runtime::job_debug_ring::forget(job_id.0);
        self.posted_name_disagreement_logged.remove(&job_id);
        self.retire_stalled_download_lanes(job_id);
        self.job_scheduling_memory.remove(&job_id);
        self.repeated_articles.remove(&job_id);
        self.job_order.retain(|id| *id != job_id);
        self.remove_pending_completion_check(job_id);
        self.pending_retries_by_job.remove(&job_id);
        self.pending_retries_by_segment
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.transport_failure_streaks
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.cancel_infrastructure_retries_for_job(job_id);
        self.download_wait_by_job.remove(&job_id);
        self.clear_terminal_segment_failures(job_id);
        self.terminal_reconciliations.remove(&job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_extraction_runtime(job_id);
        self.extraction_budgets.remove(&job_id);
        self.unacceptable_extension_policies.remove(&job_id);
        self.inflight_moves.remove(&job_id);
        self.deferred_moves.remove(&job_id);
        self.deferred_post_processing.remove(&job_id);
        self.reserved_complete_destinations.remove(&job_id);
        self.active_download_passes.remove(&job_id);
        self.jobs_finalizing_download.remove(&job_id);
        self.active_download_connections_by_job.remove(&job_id);
        self.active_completion_critical_connections_by_job
            .remove(&job_id);
        self.job_last_download_activity.remove(&job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.clear_job_progress_floor_runtime(job_id);
        self.clear_job_phase_progress_runtime(job_id);
        self.discard_stage_timers(job_id);
        self.clear_job_retention_excludes(job_id);
        self.decode_retries
            .retain(|seg_id, _| seg_id.file_id.job_id != job_id);
        self.rate_limit_reservations
            .retain(|seg_id, _| seg_id.file_id.job_id != job_id);
        self.update_queue_metrics();
    }

    // Archive the job's history row and publish `terminal_event` once that
    // archive has committed. Terminal events are routed through here so every
    // caller keeps the same guarantee: a subscriber that observes the event can
    // immediately read the row back from the history facade.
    pub(crate) fn record_job_history(
        &mut self,
        job_id: JobId,
        terminal_event: Option<PipelineEvent>,
    ) {
        if !self.jobs.get(&job_id).is_some_and(|state| {
            matches!(
                state.status,
                JobStatus::Complete | JobStatus::Partial | JobStatus::Failed { .. }
            )
        }) {
            return;
        }
        // Freeze acceptance before taking the terminal fields. An intent
        // recorded before this lock belongs to this run's terminal result.
        // A row already retiring is a cancel whose archive is in flight; its
        // done event decides the job.
        let cancelled = self.shared_state.queue.write(|inner| {
            let Some(row) = inner.jobs.iter_mut().find(|row| row.id == job_id) else {
                return crate::pipeline::queue::Change::unchanged(None);
            };
            if row.report.retiring {
                return crate::pipeline::queue::Change::unchanged(Some(None));
            }
            if let Some(origin) = row.intents.cancelled {
                return crate::pipeline::queue::Change::unchanged(Some(Some(origin)));
            }
            row.report.retiring = true;
            crate::pipeline::queue::Change::changed(None)
        });
        match cancelled {
            Some(Some(origin)) => {
                self.start_cancel_jobs(BTreeSet::from([job_id]), origin);
                return;
            }
            Some(None) => return,
            None => {}
        }
        self.apply_pending_job_update(job_id);
        // Taken before the long immutable borrow below, and before the runtime
        // that produced them is purged. A completed job carries the claim
        // census's verdict; a failed one falls through to its own counters,
        // where the raw numbers are the explanation.
        let settled = self
            .jobs
            .get(&job_id)
            .map(|state| state.spec.total_bytes)
            .map(|total_bytes| self.terminal_record_figures(job_id, total_bytes));

        let state = match self.jobs.get(&job_id) {
            Some(s) => s,
            None => {
                return;
            }
        };

        // A graph job's short delivery names the stage that left it short,
        // and a repair that could not rebuild a member says why.
        let graph_shortfall = matches!(state.status, JobStatus::Partial)
            .then(|| self.graph_shortfall(job_id))
            .flatten();
        let shortfall = graph_shortfall
            .as_ref()
            .map(|(stage, _)| *stage)
            .or_else(|| state.status.shortfall_stage());
        let (status_str, error_message) = match &state.status {
            JobStatus::Complete => ("complete".to_string(), None),
            JobStatus::Partial => (
                "partial".to_string(),
                graph_shortfall.and_then(|(_, message)| message),
            ),
            JobStatus::Failed { error } => ("failed".to_string(), Some(error.clone())),
            _ => {
                return;
            }
        };
        let completed = matches!(state.status, JobStatus::Complete | JobStatus::Partial);

        // Low-frequency: one observation per job reaching a terminal status.
        // `created_at` is an `Instant` the job state already carries, so the
        // duration costs one clock read at the end of a whole job — no
        // `SystemTime::now()` and nothing on an article path.
        self.metrics.job_lifecycle.note_finished(
            if completed {
                crate::operations::instrumentation::JobResultKind::Complete
            } else {
                crate::operations::instrumentation::JobResultKind::Failed
            },
            state.spec.category.as_deref().unwrap_or(""),
            Some(state.created_at.elapsed()),
        );

        // The job's article buffers are all free by now; the allocator is only
        // still holding their pages because it was told to recycle them within
        // a job. Nothing will ask for them again, so give them back here
        // instead of leaving a job-sized working set resident.
        crate::runtime::thread_release::release_job_completion_memory();

        let now = timestamp_secs() as i64;
        let elapsed_secs = state.created_at.elapsed().as_secs() as i64;
        let created_at = now - elapsed_secs;
        let total = state.spec.total_bytes;
        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.assembly.optional_recovery_bytes();
        let (failed_bytes, health, terminal_discards) = settled.unwrap_or_else(|| {
            (
                state.failed_bytes,
                health_milli(total, state.failed_bytes),
                Vec::new(),
            )
        });

        let row = crate::JobHistoryRow {
            job_id: job_id.0,
            job_hash: Some(state.job_hash.to_vec()),
            name: state.spec.name.clone(),
            status: status_str,
            error_message,
            total_bytes: total,
            downloaded_bytes: Self::effective_downloaded_bytes(state),
            optional_recovery_bytes,
            optional_recovery_downloaded_bytes,
            failed_bytes,
            health,
            category: state.spec.category.clone(),
            output_dir: Some(state.working_dir.display().to_string()),
            nzb_path: None,
            created_at,
            completed_at: now,
            metadata: if state.spec.metadata.is_empty() {
                None
            } else {
                serde_json::to_string(&state.spec.metadata).ok()
            },
            server_attribution: state.server_attribution.to_storage_json(),
        };

        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.assembly.optional_recovery_bytes();
        let (download_state, post_state, run_state) =
            crate::jobs::model::runtime_lanes_from_status_snapshot(&state.status);
        let finished = JobInfo {
            job_id,
            job_hash: Some(state.job_hash),
            name: state.spec.name.clone(),
            error: if let JobStatus::Failed { error } = &state.status {
                Some(error.clone())
            } else {
                None
            },
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            status: state.status.clone(),
            download_state,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state,
            run_state,
            progress: Self::effective_progress(state),
            total_bytes: total,
            downloaded_bytes: Self::effective_downloaded_bytes(state),
            optional_recovery_bytes,
            optional_recovery_downloaded_bytes,
            phase_progress: Vec::new(),
            failed_bytes,
            health,
            terminal_discards,
            total_files: state.assembly.total_file_count() as u32,
            completed_files: state.assembly.complete_file_count() as u32,
            remaining_par_files: 0,
            password: state.spec.password.clone(),
            category: state.spec.category.clone(),
            metadata: state.spec.metadata.clone(),
            output_dir: Some(state.working_dir.display().to_string()),
            server_attribution: state.server_attribution.contributions().to_vec(),
            created_at_epoch_ms: state.created_at_epoch_ms,
        };

        let whole = completed && shortfall.is_none();
        let typed_terminal_cause = if whole {
            Some(crate::jobs::SemanticTerminalCause::Success)
        } else {
            self.semantic_terminal_causes.remove(&job_id)
        };
        let failure_stage = self.pipeline_failure_stages.remove(&job_id).or(shortfall);
        let failure_kind = (!whole).then(|| {
            crate::history::HistoryFailureKind::derive(
                failure_stage,
                typed_terminal_cause,
                row.error_message.as_deref(),
                row.health,
                row.failed_bytes,
            )
        });

        self.persist_support_facts_before_archive(job_id);
        let archive_started = Instant::now();
        let archived = match self.db.try_queue_archive_job_with_terminal_cause(
            job_id,
            row,
            typed_terminal_cause,
            failure_kind,
        ) {
            Ok(archived) => archived,
            Err(e) => {
                tracing::error!(job_id = job_id.0, error = %e, "failed to queue job history archival");
                self.shared_state.queue.write(|inner| {
                    if let Some(row) = inner.jobs.iter_mut().find(|row| row.id == job_id) {
                        row.report.retiring = false;
                    }
                    crate::pipeline::queue::Change::changed(())
                });
                return;
            }
        };
        debug!(
            job_id = job_id.0,
            elapsed_ms = archive_started.elapsed().as_millis(),
            "queued job history archival"
        );
        // Purge and republish before arming the deferred event: the runtime
        // snapshot must already list the job as finished when a subscriber
        // reacts, or the history facade rejects the lookup as a still-live job.
        self.shared_state.queue.write(|inner| {
            let row = if let Some(index) = inner.jobs.iter().position(|row| row.id == job_id) {
                &mut inner.jobs[index]
            } else {
                inner
                    .jobs
                    .push(crate::pipeline::queue::JobRow::new(finished.clone()));
                inner.jobs.last_mut().expect("retiring job")
            };
            row.report.job = finished.clone();
            row.report.retiring = true;
            crate::pipeline::queue::Change::changed(())
        });
        self.purge_terminal_job_runtime(job_id);
        self.publish_terminal_event_after_archive(job_id, Some(finished), archived, terminal_event);
    }

    // Hold the terminal event until the history archive has committed, so the
    // row is queryable the moment the event is observed. A lost acknowledgement
    // retains the queue row for recovery and never publishes a false completion.
    fn publish_terminal_event_after_archive(
        &self,
        job_id: JobId,
        finished: Option<JobInfo>,
        archived: oneshot::Receiver<()>,
        terminal_event: Option<PipelineEvent>,
    ) {
        let event_tx = self.event_tx.clone();
        let queue = self.shared_state.queue.clone();
        tokio::spawn(async move {
            if archived.await.is_err() {
                tracing::error!(job_id = job_id.0, "history archive completion was lost");
                return;
            }
            queue.write(|inner| {
                inner.jobs.retain(|row| row.id != job_id);
                if let Some(job) = finished {
                    let mut history: Vec<_> = queue
                        .history()
                        .iter()
                        .filter(|job| job.job_id != job_id)
                        .cloned()
                        .collect();
                    history.insert(0, job);
                    history.truncate(crate::jobs::FINISHED_JOBS_RUNTIME_CAP);
                    queue.replace_history(inner, history.into());
                }
                if let Some(event) = terminal_event {
                    let _ = event_tx.send(event);
                }
                crate::pipeline::queue::Change::changed(())
            });
        });
    }
}
