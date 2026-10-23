// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl Pipeline {
    pub(crate) fn activate_nntp(
        &mut self,
        new_client: NntpClient,
        total_connections: usize,
        generation: u64,
    ) -> NntpRuntimeActivation {
        let new_client = Arc::new(new_client);
        let old_client = std::mem::replace(&mut self.nntp, new_client);
        self.shared_state
            .set_nntp_pool(Arc::clone(self.nntp.pool()));
        // Server indices and retention windows may have changed:
        // recompute retention and drop queued failure exclusions,
        // whose indices refer to the old pool layout. Bumping the
        // generation also invalidates the failure indices carried by
        // in-flight delayed retries when they re-enter the queue.
        self.pool_generation = generation;
        // Re-point the per-server metric counters at the new pool
        // layout. This is the only place the counter vector is
        // rebuilt, so the completion path can index it without a
        // lock; counters are keyed by stable server id, so lifetime
        // totals carry across the rebuild.
        self.server_counters = Self::activate_server_counters(&self.metrics, &self.nntp);
        // Depth explorers are keyed by pool position, so they are
        // rebuilt against the new layout. A server that survived
        // the rebuild keeps what it had measured, matched by
        // stable id rather than by position.
        self.seed_download_lane_explorers();
        let recovery_requeues = self.wake_all_infrastructure_retries();
        self.reset_owned_download_lanes();
        self.drop_held_download_refills();
        self.clear_retention_exclude_cache();
        for state in self.jobs.values_mut() {
            state.download_queue.clear_exclude_servers();
            state.recovery_queue.clear_exclude_servers();
        }
        self.metrics
            .nntp_generation_recovery_requeues
            .fetch_add(recovery_requeues as u64, Ordering::Relaxed);
        self.owned_download_lane_pool
            .resize(total_connections.max(1));
        self.tuner.set_connection_limit(total_connections);
        // Dispatch on the new pool only once the old one has let
        // go of its sockets. The provider counts both generations
        // against one allowance, so dialling into the overlap is
        // refused as "too many connections" and would hold the
        // new pool off for the whole window on a healthy server.
        // Old-generation lanes drain on their own: their refills
        // are answered with the generation error and they park.
        self.nntp_handoff_draining = true;
        let drained_tx = self.nntp_handoff_drained_tx.clone();
        let generation = self.pool_generation;
        tokio::spawn(async move {
            old_client.shutdown().await;
            let _ = drained_tx.send(generation).await;
        });
        let activation = NntpRuntimeActivation {
            generation: self.pool_generation,
            configured_connections: total_connections,
        };
        self.shared_state.set_nntp_runtime_activation(activation);
        info!(
            runtime_generation = activation.generation,
            configured_connections = activation.configured_connections,
            recovery_requeues,
            max_downloads = self.tuner.params().max_concurrent_downloads,
            "NNTP runtime generation activated"
        );
        activation
    }

    pub(crate) async fn rename_working_dir_when_quiet(
        &mut self,
        job_id: JobId,
        name: &str,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        if self
            .active_downloads_by_job
            .get(&job_id)
            .is_some_and(|n| *n > 0)
            || self
                .active_decodes_by_job
                .get(&job_id)
                .is_some_and(|n| *n > 0)
            || self
                .direct_barrier_flights
                .keys()
                .any(|(id, _)| *id == job_id)
            || self
                .direct_placement_lanes
                .keys()
                .any(|(id, _)| *id == job_id)
            || self
                .direct_rearm_in_flight
                .iter()
                .any(|(id, _)| *id == job_id)
            || self.direct_post_repair_in_flight.contains_key(&job_id)
            || self.direct_tolerated_in_flight.contains_key(&job_id)
            || self.direct_demotion_in_flight.contains_key(&job_id)
            || self.par2_analysis_in_flight.contains_key(&job_id)
            || self.inflight_extractions.contains_key(&job_id)
            || self.has_active_rar_workers(job_id)
            || self.inflight_terminal_post_processing.contains(&job_id)
            || self.added_script_holds.contains(&job_id)
            || self.inflight_moves.contains(&job_id)
            || self.direct_unpack.has_job_work_in_flight(job_id)
            || self
                .par3_runtime
                .as_ref()
                .is_some_and(|runtime| runtime.has_worker_in_flight(job_id))
            || self
                .rar_refresh_state
                .iter()
                .any(|((id, _), state)| *id == job_id && state.in_flight.is_some())
        {
            return false;
        }
        let old_dir = state.working_dir.clone();
        if old_dir.parent() != Some(self.intermediate_dir.as_path()) {
            return true;
        }
        if old_dir.file_name()
            == Some(std::ffi::OsStr::new(
                &crate::jobs::working_dir::sanitize_dirname(name),
            ))
        {
            return true;
        }
        let new_dir =
            crate::jobs::working_dir::compute_working_dir(&self.intermediate_dir, job_id, name);
        if new_dir == old_dir {
            return true;
        }
        crate::pipeline::close_cached_write_handles_under(&old_dir).await;
        let root = self.intermediate_dir.clone();
        let source = old_dir.clone();
        let result = self
            .db_blocking(move |db| {
                crate::jobs::working_dir::rename_job_working_dir(
                    db, &root, &source, &new_dir, job_id,
                )
            })
            .await;
        let new_dir = match result {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!(job_id = job_id.0, %error, "working directory rename remains pending");
                return false;
            }
        };
        self.direct_store
            .relocate_working_dir(job_id, &old_dir, &new_dir);
        self.invalidate_par2_session_for_identity_rebind(job_id);
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            for set in runtime.sets.values_mut() {
                set.scan_carry = None;
                set.scan_carry_exclusions.clear();
            }
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.working_dir = new_dir;
        }
        true
    }

    pub(crate) async fn cleanup_cancelled_job_directories(
        db: &crate::Database,
        job_id: JobId,
        working_dir: &std::path::Path,
        staging_dir: Option<&std::path::Path>,
    ) -> Result<(), crate::StateError> {
        // Deletion scripts still need this cwd after archival. A cancelled
        // queue run must also leave its process group before files disappear.
        crate::post_processing::events::wait_for_job_events_stopped(db, job_id.0).await?;
        // A re-added job may reuse either path, so release cached handles before
        // removing its previous working and direct-store staging directories.
        crate::pipeline::close_cached_write_handles_under(working_dir).await;
        if let Some(staging) = staging_dir {
            crate::pipeline::close_cached_write_handles_under(staging).await;
        }
        if let Err(error) = tokio::fs::remove_dir_all(working_dir).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(dir = %working_dir.display(), %error, "failed to clean up cancelled job directory");
        }
        if let Some(staging) = staging_dir
            && let Err(error) = tokio::fs::remove_dir_all(staging).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(dir = %staging.display(), %error, "failed to clean up cancelled job staging directory");
        }
        Ok(())
    }

    // The pool replaced by generation `generation` has drained its sockets,
    // so the new pool may dial without competing for the provider allowance.
    //
    // A drain that finishes after a further rebuild is stale: only the newest
    // generation's own drain re-opens dispatch, which keeps the overlap rule
    // intact across back-to-back settings saves.
    pub(crate) fn handle_nntp_handoff_drained(&mut self, generation: u64) {
        if generation != self.pool_generation {
            debug!(
                drained_generation = generation,
                runtime_generation = self.pool_generation,
                "ignoring a stale NNTP generation drain"
            );
            return;
        }
        if !self.nntp_handoff_draining {
            return;
        }
        self.nntp_handoff_draining = false;
        info!(
            runtime_generation = generation,
            "previous NNTP generation drained; dispatching on the new pool"
        );
        self.dispatch_downloads();
    }
}

// What a cancelled job's directory cleanup waits for and removes, carried
// from the turn that takes the cancel to the turn that handles its archive.
struct CancelledJobCleanup {
    job_id: JobId,
    post_processing_done: Option<tokio::sync::watch::Receiver<bool>>,
    extraction_cleanup_wait: Option<Arc<JobExtractionBudget>>,
    working_dir: PathBuf,
    staging_dir: Option<PathBuf>,
}

// A cancel batch's archive outcome, carried back to the actor once the
// archive write has answered.
pub(crate) struct CancelJobsDone {
    cleanups: Vec<CancelledJobCleanup>,
    // The ids whose archive failed and why; an error means the writer closed
    // before answering.
    archived: Result<BTreeMap<JobId, String>, oneshot::error::RecvError>,
    keep_files: bool,
}

impl Pipeline {
    // Take accepted cancellation intents as one archive batch. Every job
    // keeps its runtime until its own archive commits; the done event that
    // follows retires the committed ones and hands the rest back.
    pub(super) fn start_cancel_jobs(
        &mut self,
        job_ids: BTreeSet<JobId>,
        origin: crate::jobs::handle::CancellationOrigin,
    ) {
        let mut archives = Vec::new();
        let mut cleanups = Vec::new();
        for job_id in job_ids {
            match self.begin_cancel_job(job_id, origin) {
                Ok((row, cleanup)) => {
                    archives.push((job_id, row));
                    cleanups.push(cleanup);
                }
                Err(error) => {
                    tracing::error!(job_id = job_id.0, %error, "failed to start accepted cancellation");
                }
            }
        }
        if archives.is_empty() {
            return;
        }
        let ids: Vec<JobId> = cleanups.iter().map(|cleanup| cleanup.job_id).collect();
        let failure_kind = (origin == crate::jobs::handle::CancellationOrigin::UserDuplicate)
            .then_some(crate::history::HistoryFailureKind::Duplicate);
        let committed = match self.db.try_queue_archive_jobs(archives, failure_kind) {
            Ok(committed) => committed,
            Err(error) => {
                // Nothing was archived: every job stays live and recoverable,
                // and a further cancel needs a new intent.
                tracing::error!(error = %error, "failed to queue cancelled job archives");
                self.release_cancel_intents(&ids);
                return;
            }
        };
        // Taken: the rows are being archived, and these intents are acted on.
        self.shared_state.queue.write(|inner| {
            for row in inner.jobs.iter_mut().filter(|row| ids.contains(&row.id)) {
                row.report.retiring = true;
                row.report.seen_intent_seq = row.intents.seq;
            }
            crate::pipeline::queue::Change::changed(())
        });
        let keep_files = matches!(
            origin,
            crate::jobs::handle::CancellationOrigin::UserKeepingFiles
        );
        let completed = self.terminal_post_processing_done_tx.clone();
        tokio::spawn(async move {
            let archived = committed.await;
            let _ = completed
                .send(TerminalPostProcessingEvent::CancelJobsDone(
                    CancelJobsDone {
                        cleanups,
                        archived,
                        keep_files,
                    },
                ))
                .await;
        });
    }

    // The archive has answered. Each id it committed leaves memory and the
    // Queue with one `JobCancelled`; an id whose archive failed, or every id
    // when the writer closed first, is live again.
    pub(in crate::pipeline) fn handle_cancel_jobs_done(&mut self, done: CancelJobsDone) {
        let CancelJobsDone {
            cleanups,
            archived,
            keep_files,
        } = done;
        let mut failed = Vec::new();
        let mut cancelled = Vec::new();
        for mut cleanup in cleanups {
            let job_id = cleanup.job_id;
            let failure = match &archived {
                Ok(errors) => errors.get(&job_id).cloned(),
                Err(_) => Some("the history writer closed before answering".to_string()),
            };
            if let Some(error) = failure {
                tracing::error!(job_id = job_id.0, %error, "failed to durably archive cancelled job");
                failed.push(job_id);
                continue;
            }
            if let Some(state) = self.purge_cancelled_job_runtime(job_id) {
                cleanup.working_dir = state.working_dir;
                cleanup.staging_dir = state.staging_dir;
            }
            cancelled.push(cleanup);
        }
        self.release_cancel_intents(&failed);
        if cancelled.is_empty() {
            return;
        }
        // Same boundary as a job that completed or failed: the cancelled
        // jobs' article pages are free and unwanted, so hand them back rather
        // than holding a job-sized working set until the purge clock runs out.
        crate::runtime::thread_release::release_job_completion_memory();
        let queue = self.shared_state.queue.clone();
        for cleanup in cancelled {
            let job_id = cleanup.job_id;
            let delete_history = queue.write(|inner| {
                let delete_history = inner
                    .jobs
                    .iter()
                    .any(|row| row.id == job_id && row.intents.delete_history_after_cancel);
                if !keep_files {
                    inner.pending_cleanup.insert(job_id);
                }
                inner.jobs.retain(|row| row.id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCancelled { job_id });
                crate::pipeline::queue::Change::changed(delete_history)
            });
            if delete_history {
                let history = queue.clone();
                tokio::spawn(async move {
                    let result = history
                        .delete_history(BTreeSet::from([job_id]), false)
                        .await
                        .and_then(|mut outcome| {
                            outcome.refused.remove(&job_id).map_or(Ok(()), Err)
                        });
                    if let Err(error) = result {
                        tracing::error!(job_id = job_id.0, %error, "failed to finish requested history deletion after cancellation");
                    }
                });
            }
            // A cancel that keeps the files schedules nothing.
            if !keep_files {
                let cleanup_db = self.db.clone();
                let queue = queue.clone();
                tokio::spawn(async move {
                    Self::cleanup_cancelled_job(cleanup_db, cleanup).await;
                    queue.write(|inner| {
                        inner.pending_cleanup.remove(&job_id);
                        crate::pipeline::queue::Change::changed(())
                    });
                });
            }
        }
    }

    // A cancel whose archive did not commit hands the job back: the row is
    // visible again in the same write that withdraws the cancel, and a
    // further cancel needs a new intent. Whatever the job finished while it
    // waited is judged now.
    fn release_cancel_intents(&mut self, ids: &[JobId]) {
        if ids.is_empty() {
            return;
        }
        self.shared_state.queue.write(|inner| {
            for row in inner.jobs.iter_mut().filter(|row| ids.contains(&row.id)) {
                row.intents.cancelled = None;
                row.intents.delete_history_after_cancel = false;
                row.report.retiring = false;
            }
            crate::pipeline::queue::Change::changed(())
        });
        for &job_id in ids {
            self.restore_after_failed_cancellation(job_id);
            self.report_job(job_id);
        }
    }

    // The archive did not commit, so future work belongs to a live job again.
    // Tokens already handed to workers stay cancelled. Discard their read-only
    // analysis tickets and retained sessions so neither a late verdict nor a
    // cached session can reintroduce the cancelled token into the next pass.
    fn restore_after_failed_cancellation(&mut self, job_id: JobId) {
        self.forget_par2_analysis_work(job_id);
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            runtime.scan_budget = None;
            for set in runtime.sets.values_mut() {
                set.session = None;
                set.session_last_used = None;
                set.session_evidence_file_ids.clear();
            }
        }
        self.par2_cancellations.remove(&job_id);
        self.shared_state.clear_job_cancellations(job_id);
        self.schedule_job_completion_check(job_id);
    }

    // Send cancellation signals and prepare history for an accepted intent.
    // Runtime ownership stays here until the job's archive has committed.
    fn begin_cancel_job(
        &mut self,
        job_id: JobId,
        origin: crate::jobs::handle::CancellationOrigin,
    ) -> Result<(crate::JobHistoryRow, CancelledJobCleanup), crate::SchedulerError> {
        // Refusal was decided atomically when the API recorded this intent.
        // A later phase transition must not revoke an accepted cancellation.
        if !self.jobs.contains_key(&job_id) {
            return Err(crate::SchedulerError::JobNotFound(job_id));
        }
        if let Some(update) = self.shared_state.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .find(|row| row.id == job_id)
                .and_then(|row| row.intents.update.clone())
        }) {
            update.apply_to_spec(&mut self.jobs.get_mut(&job_id).expect("live cancellation").spec);
        }
        self.db.cancel_event_scripts(job_id.0);
        if origin.is_user() {
            // Reassert after any earlier failed cancellation withdrew its signal.
            self.shared_state.cancel_active_job_work(job_id);
            self.raise_queue_script_event(
                job_id,
                crate::post_processing::model::QueueEvent::NzbDeleted,
                Some(
                    if origin == crate::jobs::handle::CancellationOrigin::UserDuplicate {
                        "DUPE"
                    } else {
                        "MANUAL"
                    },
                ),
            );
        }
        // Normal job cancellation must also interrupt terminal
        // post-processing. The pipeline-level signal covers a run
        // waiting for admission; the executor-level signal covers a
        // script that is already running.
        let post_processing_done = self
            .terminal_post_processing_cancellations
            .get(&job_id)
            .map(tokio::sync::watch::Sender::subscribe);
        let pipeline_cancelled = self
            .terminal_post_processing_cancellations
            .get(&job_id)
            .is_some_and(|sender| sender.send(true).is_ok());
        let executor_cancelled = self.terminal_post_processing_executor.cancel_job(job_id.0);
        let par2_cancelled = if let Some(cancellation) = self.par2_cancellations.get(&job_id) {
            cancellation.cancel();
            true
        } else {
            false
        };
        let extraction_cleanup_wait = self.extraction_budgets.get(&job_id).cloned();
        let extraction_cancelled = if let Some(budget) = &extraction_cleanup_wait {
            budget.cancel();
            true
        } else {
            false
        };
        if pipeline_cancelled || executor_cancelled {
            tracing::debug!(
                job_id = job_id.0,
                "requested cancellation of active post-processing"
            );
        }
        if par2_cancelled || extraction_cancelled {
            tracing::debug!(
                job_id = job_id.0,
                par2_cancelled,
                extraction_cancelled,
                "requested cancellation of active repair or extraction"
            );
        }
        // Ahead of the archive, which copies them from the active row: a job
        // write that the batch's archive, queued after it, waits out.
        self.persist_support_facts_before_archive(job_id);
        let state = self
            .jobs
            .get(&job_id)
            .expect("contains_key checked immediately above");
        let now = timestamp_secs() as i64;
        let elapsed_secs = state.created_at.elapsed().as_secs() as i64;
        let created_at = now - elapsed_secs;
        let total = state.spec.total_bytes;
        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.assembly.optional_recovery_bytes();
        let health = health_milli(total, state.failed_bytes);
        let row = crate::JobHistoryRow {
            job_id: job_id.0,
            job_hash: Some(state.job_hash.to_vec()),
            name: state.spec.name.clone(),
            status: "cancelled".to_string(),
            // History has no Cancelled state: the row reads Failed, so it
            // names who ended it.
            error_message: origin
                .is_user()
                .then(|| crate::jobs::model::OPERATOR_CANCELLED_ERROR.to_string()),
            total_bytes: total,
            downloaded_bytes: Self::effective_downloaded_bytes(state),
            optional_recovery_bytes,
            optional_recovery_downloaded_bytes,
            failed_bytes: state.failed_bytes,
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
            // A cancelled job keeps whatever it managed to pull
            // before the cancel: the servers did the work, and the
            // row is the only place left to say so.
            server_attribution: state.server_attribution.to_storage_json(),
        };
        Ok((
            row,
            CancelledJobCleanup {
                job_id,
                post_processing_done,
                extraction_cleanup_wait,
                working_dir: state.working_dir.clone(),
                staging_dir: state.staging_dir.clone(),
            },
        ))
    }

    // Drops a cancelled job's runtime state once its archive has committed.
    fn purge_cancelled_job_runtime(&mut self, job_id: JobId) -> Option<JobState> {
        let state = self.jobs.remove(&job_id)?;
        self.retire_stalled_download_lanes(job_id);
        self.job_order.retain(|id| *id != job_id);
        self.remove_pending_completion_check(job_id);
        self.update_queue_metrics();

        self.clear_par2_runtime_state(job_id);
        self.clear_job_extraction_runtime(job_id);
        self.active_download_passes.remove(&job_id);
        self.jobs_finalizing_download.remove(&job_id);
        self.active_download_connections_by_job.remove(&job_id);
        self.active_completion_critical_connections_by_job
            .remove(&job_id);
        self.active_decodes_by_job.remove(&job_id);
        self.active_decodes_by_file
            .retain(|file_id, _| file_id.job_id != job_id);
        self.active_decode_bytes
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.pending_retries_by_job.remove(&job_id);
        self.pending_retries_by_segment
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.transport_failure_streaks
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.cancel_infrastructure_retries_for_job(job_id);
        self.download_wait_by_job.remove(&job_id);
        self.segment_terminal_states
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.rate_limit_reservations
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.job_last_download_activity.remove(&job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.clear_job_progress_floor_runtime(job_id);
        self.clear_job_phase_progress_runtime(job_id);
        self.discard_stage_timers(job_id);
        self.clear_job_retention_excludes(job_id);

        // Low-frequency: one observation per cancellation. The
        // elapsed span comes from the `Instant` the job state
        // already carries, so no extra wall-clock source is
        // introduced.
        self.metrics.job_lifecycle.note_finished(
            crate::operations::instrumentation::JobResultKind::Cancelled,
            state.spec.category.as_deref().unwrap_or(""),
            Some(state.created_at.elapsed()),
        );
        Some(state)
    }

    async fn cleanup_cancelled_job(db: crate::Database, cleanup: CancelledJobCleanup) {
        let job_id = cleanup.job_id;
        // Let a cancelled post-processing script leave its
        // process group before its working directory is
        // removed. The sender is dropped by the terminal
        // completion handler once that run has ended.
        if let Some(mut post_processing_done) = cleanup.post_processing_done {
            while post_processing_done.changed().await.is_ok() {}
        }
        if let Some(extraction_budget) = cleanup.extraction_cleanup_wait {
            let _ = tokio::task::spawn_blocking(move || {
                extraction_budget.wait_for_idle();
            })
            .await;
        }
        if let Err(error) = Self::cleanup_cancelled_job_directories(
            &db,
            job_id,
            &cleanup.working_dir,
            cleanup.staging_dir.as_deref(),
        )
        .await
        {
            tracing::warn!(job_id = job_id.0, %error, "retaining cancelled job directory while queue scripts are unresolved");
        }
    }
}
