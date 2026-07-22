use super::*;

const NNTP_ACTIVATION_OVERLAP_GRACE: Duration = Duration::from_secs(30);

impl Pipeline {
    pub(crate) async fn handle_command(&mut self, cmd: SchedulerCommand) {
        match cmd {
            SchedulerCommand::AddJob {
                job_id,
                spec,
                nzb_path,
                nzb_zstd,
                options,
                reply,
            } => {
                let result = self
                    .add_job(job_id, spec, nzb_path, nzb_zstd, options)
                    .await;
                if result.is_ok() {
                    self.publish_snapshot();
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::RestoreJob { request, reply } => {
                let result = self.restore_job(*request).await;
                if result.is_ok() {
                    self.publish_snapshot();
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::PauseJob { job_id, reply } => {
                let result = self.pause_job_runtime(job_id);
                if result.is_ok() {
                    self.publish_snapshot();
                    self.emit_download_finished_if_active(job_id);
                    let _ = self.event_tx.send(PipelineEvent::JobPaused { job_id });
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::ResumeJob { job_id, reply } => {
                let result = self.resume_job_runtime(job_id);
                if result.is_ok() {
                    self.publish_snapshot();
                    let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::CancelJob {
                job_id,
                origin,
                reply,
            } => {
                let semantic_cancel_is_safe = self.jobs.get(&job_id).is_some_and(|state| {
                    matches!(
                        (state.download_state, state.post_state),
                        (
                            crate::jobs::model::DownloadState::Queued
                                | crate::jobs::model::DownloadState::Downloading,
                            crate::jobs::model::PostState::Idle
                        )
                    )
                });
                let result = if !matches!(origin, crate::jobs::handle::CancellationOrigin::User)
                    && !semantic_cancel_is_safe
                {
                    Err(crate::SchedulerError::Conflict(
                        "semantic cancellation is only allowed while queued or downloading"
                            .to_string(),
                    ))
                } else if self.inflight_moves.contains(&job_id) {
                    Err(crate::SchedulerError::Conflict(
                        "cancel is not supported while the final move is running".to_string(),
                    ))
                } else if self.jobs.contains_key(&job_id) {
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
                        error_message: None,
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
                    };
                    let archive_result = self
                        .db_blocking({
                            move |db| {
                                db.archive_job(job_id, &row)
                                    .map_err(|e| format!("failed to archive cancelled job: {e}"))?;
                                Ok::<(), String>(())
                            }
                        })
                        .await;
                    if let Err(error) = archive_result {
                        tracing::error!(job_id = job_id.0, error = %error, "failed to durably archive cancelled job");
                        Err(crate::SchedulerError::State(crate::StateError::Database(
                            format!("failed to archive cancelled job: {error}"),
                        )))
                    } else {
                        // Preserve the live job until its cancellation archive and
                        // semantic terminal state are durable. A failed semantic
                        // supersession must remain recoverable rather than report a
                        // successful cancellation.
                        let state = self
                            .jobs
                            .remove(&job_id)
                            .expect("job was retained until archive completed");
                        self.job_order.retain(|id| *id != job_id);
                        self.remove_pending_completion_check(job_id);
                        self.update_queue_metrics();

                        self.clear_par2_runtime_state(job_id);
                        self.clear_job_extraction_runtime(job_id);
                        self.active_download_passes.remove(&job_id);
                        self.jobs_finalizing_download.remove(&job_id);
                        self.active_downloads_by_job.remove(&job_id);
                        self.active_download_connections_by_job.remove(&job_id);
                        self.active_downloads_by_file
                            .retain(|file_id, _| file_id.job_id != job_id);
                        self.active_decodes_by_job.remove(&job_id);
                        self.active_decodes_by_file
                            .retain(|file_id, _| file_id.job_id != job_id);
                        self.pending_retries_by_job.remove(&job_id);
                        self.pending_retries_by_segment
                            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
                        self.cancel_infrastructure_retries_for_job(job_id);
                        self.download_wait_by_job.remove(&job_id);
                        self.terminal_segment_failures
                            .retain(|segment_id| segment_id.file_id.job_id != job_id);
                        self.rate_limit_reservations
                            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
                        self.job_last_download_activity.remove(&job_id);
                        self.clear_job_rar_runtime(job_id);
                        self.clear_job_write_backlog(job_id);
                        self.clear_job_progress_floor_runtime(job_id);
                        self.clear_job_phase_progress_runtime(job_id);
                        self.clear_job_retention_excludes(job_id);

                        let working_dir = state.working_dir.clone();
                        let staging_dir = state.staging_dir.clone();
                        tokio::spawn(async move {
                            // Close cached write handles first: the working-dir
                            // path may be reused verbatim by a re-added job, and a
                            // stale handle would swallow its writes.
                            crate::pipeline::close_cached_write_handles_under(&working_dir).await;
                            if let Err(e) = tokio::fs::remove_dir_all(&working_dir).await
                                && e.kind() != std::io::ErrorKind::NotFound
                            {
                                tracing::warn!(
                                    dir = %working_dir.display(),
                                    error = %e,
                                    "failed to clean up cancelled job directory"
                                );
                            }
                            if let Some(staging) = staging_dir
                                && let Err(e) = tokio::fs::remove_dir_all(&staging).await
                                && e.kind() != std::io::ErrorKind::NotFound
                            {
                                tracing::warn!(
                                    dir = %staging.display(),
                                    error = %e,
                                    "failed to clean up cancelled job staging directory"
                                );
                            }
                        });

                        Ok(())
                    }
                } else {
                    Err(crate::SchedulerError::JobNotFound(job_id))
                };
                if result.is_ok() {
                    self.publish_snapshot();
                    let _ = self.event_tx.send(PipelineEvent::JobCancelled { job_id });
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::UpdateJob {
                job_id,
                update,
                reply,
            } => {
                let result = if let Some(state) = self.jobs.get_mut(&job_id) {
                    update.apply_to_spec(&mut state.spec);
                    // Ordered writer queue, not fire-and-forget: category/metadata
                    // are last-write-wins columns, so two quick UpdateJob calls
                    // must persist in order to avoid a stale restored value.
                    if let Err(e) = self.db.try_queue_write("update_active_job", move |db| {
                        db.update_active_job(job_id, &update)
                    }) {
                        error!(error = %e, "failed to queue UpdateJob write");
                    }
                    self.publish_snapshot();
                    Ok(())
                } else {
                    Err(crate::SchedulerError::JobNotFound(job_id))
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::ReorderJob {
                job_id,
                target,
                reply,
            } => {
                let result =
                    if self.jobs.contains_key(&job_id) {
                        crate::jobs::handle::splice_job_order(&mut self.job_order, job_id, target)
                            .map(|moved| {
                                if !moved {
                                    return;
                                }
                                // Persist the whole (small) order so restore can
                                // sort by queue_position and reproduce it exactly.
                                let positions: Vec<(JobId, i64)> = self
                                    .job_order
                                    .iter()
                                    .enumerate()
                                    .map(|(index, id)| (*id, index as i64))
                                    .collect();
                                if let Err(e) = self.db.try_queue_write(
                                    "update_active_job_queue_positions",
                                    move |db| db.update_active_job_queue_positions(&positions),
                                ) {
                                    error!(error = %e, "failed to queue ReorderJob write");
                                }
                                self.publish_snapshot();
                            })
                    } else {
                        Err(crate::SchedulerError::JobNotFound(job_id))
                    };
                let _ = reply.send(result);
            }
            SchedulerCommand::ReorderJobs { moves, reply } => {
                // All-or-nothing: if any job id is unknown, apply nothing so
                // the facade can map JobNotFound -> false without a partially
                // applied batch.
                let unknown_job = moves
                    .iter()
                    .map(|(job_id, _)| *job_id)
                    .find(|job_id| !self.jobs.contains_key(job_id));
                let result = if let Some(job_id) = unknown_job {
                    Err(crate::SchedulerError::JobNotFound(job_id))
                } else {
                    let mut any_moved = false;
                    for (job_id, target) in moves {
                        match crate::jobs::handle::splice_job_order(
                            &mut self.job_order,
                            job_id,
                            target,
                        ) {
                            Ok(moved) => any_moved |= moved,
                            Err(e) => {
                                // Every id was verified present in `self.jobs`
                                // above, so this would only fire on an
                                // internal inconsistency between `self.jobs`
                                // and `self.job_order`. Log and keep applying
                                // the remaining moves rather than aborting
                                // the whole batch partway through.
                                error!(
                                    job_id = job_id.0,
                                    error = %e,
                                    "reorder_jobs: job missing from manual order despite presence check"
                                );
                            }
                        }
                    }
                    if any_moved {
                        // Persist the whole (small) order ONCE for the batch —
                        // not once per move — so restore can sort by
                        // queue_position and reproduce it exactly.
                        let positions: Vec<(JobId, i64)> = self
                            .job_order
                            .iter()
                            .enumerate()
                            .map(|(index, id)| (*id, index as i64))
                            .collect();
                        if let Err(e) = self
                            .db
                            .try_queue_write("update_active_job_queue_positions", move |db| {
                                db.update_active_job_queue_positions(&positions)
                            })
                        {
                            error!(error = %e, "failed to queue ReorderJobs write");
                        }
                        self.publish_snapshot();
                    }
                    Ok(())
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::PauseAll { reply } => {
                self.global_paused = true;
                self.shared_state.set_paused(true);
                self.shared_state
                    .set_download_block(self.bandwidth_cap.to_download_block_state(true));
                if let Err(e) = self
                    .db_blocking(move |db| db.set_setting("global_paused", "true"))
                    .await
                {
                    error!(error = %e, "db write failed for PauseAll");
                }
                let _ = self.event_tx.send(PipelineEvent::GlobalPaused);
                let _ = reply.send(());
            }
            SchedulerCommand::ResumeAll { reply } => {
                self.global_paused = false;
                self.shared_state.set_paused(false);
                let _ = self.refresh_bandwidth_cap_window();
                if let Err(e) = self
                    .db_blocking(move |db| db.set_setting("global_paused", "false"))
                    .await
                {
                    error!(error = %e, "db write failed for ResumeAll");
                }
                let _ = self.event_tx.send(PipelineEvent::GlobalResumed);
                let _ = reply.send(());
            }
            SchedulerCommand::SetSpeedLimit {
                bytes_per_sec,
                reply,
            } => {
                self.rate_limiter.set_rate(bytes_per_sec);
                let _ = reply.send(());
            }
            SchedulerCommand::SetIpReplacementTrialExtraConnections {
                extra_connections,
                reply,
            } => {
                self.ip_replacement_trial_extra_connections = extra_connections.min(1);
                if self.ip_replacement_trial_extra_connections == 0 {
                    self.ip_replacement_burst_active = false;
                    self.ip_rtt_ewma.clear();
                    self.ip_replacement_retired_ips.clear();
                    self.metrics.set_ip_replacement_burst_active(false);
                    self.metrics.set_ip_rtt_ewma_summary(0, 0);
                }
                self.metrics.set_ip_replacement_trial_extra_connections(
                    self.ip_replacement_trial_extra_connections,
                );
                let _ = reply.send(());
            }
            SchedulerCommand::SetBandwidthCapPolicy { policy, reply } => {
                let result = self.apply_bandwidth_cap_policy(policy);
                let _ = reply.send(result);
            }
            SchedulerCommand::ApplyScheduleAction { action, reply } => {
                use crate::bandwidth::ScheduleAction;
                match action {
                    ScheduleAction::Pause => {
                        self.global_paused = true;
                        self.shared_state.set_paused(true);
                        let mut block = self.bandwidth_cap.to_download_block_state(true);
                        block.kind = crate::jobs::handle::DownloadBlockKind::Scheduled;
                        self.shared_state.set_download_block(block);
                        info!("schedule: paused downloads");
                    }
                    ScheduleAction::Resume => {
                        self.global_paused = false;
                        self.shared_state.set_paused(false);
                        let _ = self.refresh_bandwidth_cap_window();
                        info!("schedule: resumed downloads");
                    }
                    ScheduleAction::SpeedLimit { bytes_per_sec } => {
                        self.rate_limiter.set_rate(bytes_per_sec);
                        let mut block = self
                            .bandwidth_cap
                            .to_download_block_state(self.global_paused);
                        block.scheduled_speed_limit = bytes_per_sec;
                        self.shared_state.set_download_block(block);
                        info!(bytes_per_sec, "schedule: set speed limit");
                    }
                    ScheduleAction::PauseWatchFolderScanning
                    | ScheduleAction::ResumeWatchFolderScanning => {
                        warn!(
                            action = ?action,
                            "watch folder schedule action reached download pipeline"
                        );
                    }
                }
                let _ = reply.send(());
            }
            SchedulerCommand::ClearScheduleAction { reply } => {
                if self.global_paused {
                    self.global_paused = false;
                    self.shared_state.set_paused(false);
                }
                self.rate_limiter.set_rate(0);
                let _ = self.refresh_bandwidth_cap_window();
                info!("schedule: cleared scheduled action");
                let _ = reply.send(());
            }
            SchedulerCommand::RebuildNntp {
                client,
                total_connections,
                reply,
            } => {
                let result = if let Ok(new_client) = client.downcast::<NntpClient>() {
                    let new_client = Arc::new(*new_client);
                    new_client
                        .pool()
                        .begin_activation_overlap_grace(NNTP_ACTIVATION_OVERLAP_GRACE);
                    let effective_connections = new_client.pool().effective_connection_capacity();
                    let old_client = std::mem::replace(&mut self.nntp, new_client);
                    // Server indices and retention windows may have changed:
                    // recompute retention and drop queued failure exclusions,
                    // whose indices refer to the old pool layout. Bumping the
                    // generation also invalidates the failure indices carried by
                    // in-flight delayed retries when they re-enter the queue.
                    self.pool_generation = self.pool_generation.wrapping_add(1);
                    let recovery_requeues = self.wake_all_infrastructure_retries();
                    self.clear_retention_exclude_cache();
                    for state in self.jobs.values_mut() {
                        state.download_queue.clear_exclude_servers();
                        state.recovery_queue.clear_exclude_servers();
                    }
                    self.metrics
                        .nntp_generation_recovery_requeues
                        .fetch_add(recovery_requeues as u64, Ordering::Relaxed);
                    self.owned_download_lane_pool.reset();
                    self.owned_download_lane_pool
                        .resize(total_connections.max(1));
                    self.connection_ramp = total_connections.min(5);
                    self.tuner.set_connection_limit(total_connections);
                    tokio::spawn(async move { old_client.shutdown().await });
                    self.dispatch_downloads();
                    let activation = NntpRuntimeActivation {
                        generation: self.pool_generation,
                        configured_connections: total_connections,
                        effective_connections,
                    };
                    self.shared_state.set_nntp_runtime_activation(activation);
                    self.publish_snapshot();
                    info!(
                        runtime_generation = activation.generation,
                        configured_connections = activation.configured_connections,
                        effective_connections = activation.effective_connections,
                        recovery_requeues,
                        max_downloads = self.tuner.params().max_concurrent_downloads,
                        "NNTP runtime generation activated"
                    );
                    Ok(activation)
                } else {
                    Err(SchedulerError::Internal(
                        "invalid NNTP runtime client supplied for activation".to_string(),
                    ))
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::UpdateRuntimePaths {
                data_dir,
                intermediate_dir,
                complete_dir,
                reply,
            } => {
                let result = std::fs::create_dir_all(&data_dir)
                    .and_then(|_| std::fs::create_dir_all(&intermediate_dir))
                    .and_then(|_| std::fs::create_dir_all(&complete_dir))
                    .map_err(crate::SchedulerError::Io)
                    .map(|_| {
                        self.intermediate_dir = intermediate_dir;
                        self.complete_dir = complete_dir;
                        self.nzb_dir = data_dir.join(".weaver-nzbs");
                    });
                let _ = reply.send(result);
            }
            SchedulerCommand::ReprocessJob { job_id, reply } => {
                let result = self.reprocess_job(job_id).await;
                if result.is_ok() {
                    self.publish_snapshot();
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::RedownloadJob { job_id, reply } => {
                let result = self.redownload_job(job_id).await;
                if result.is_ok() {
                    self.publish_snapshot();
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::DeleteHistory {
                job_id,
                delete_files,
                reply,
            } => {
                let history_cleanup_dirs = match self.history_cleanup_dirs_for_job(job_id).await {
                    Ok(dirs) => dirs,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                };
                let output_dir = if delete_files {
                    self.output_dir_for_job(job_id).await
                } else {
                    None
                };
                let result = match self.jobs.get(&job_id).map(|state| state.status.clone()) {
                    Some(status) if !is_terminal_status(&status) => {
                        Err(crate::SchedulerError::Conflict(
                            "cannot delete active job — cancel it first".into(),
                        ))
                    }
                    Some(_) => {
                        if let Err(error) = self
                            .cleanup_history_intermediate_dirs(&history_cleanup_dirs)
                            .await
                        {
                            let _ = reply.send(Err(error));
                            return;
                        }
                        self.cleanup_output_dir(output_dir.as_deref()).await;
                        self.purge_terminal_job_runtime(job_id);
                        self.finished_jobs.retain(|j| j.job_id != job_id);
                        let db = self.db.clone();
                        tokio::task::spawn_blocking(move || {
                            if let Err(error) = db.delete_job_history(job_id.0) {
                                tracing::warn!(
                                    job_id = job_id.0,
                                    error = %error,
                                    "failed to delete job history row on user delete"
                                );
                            }
                            if let Err(error) = db.delete_job_events(job_id.0) {
                                tracing::warn!(
                                    job_id = job_id.0,
                                    error = %error,
                                    "failed to delete job events on user delete"
                                );
                            }
                        });
                        self.publish_snapshot();
                        Ok(())
                    }
                    None => {
                        if let Err(error) = self
                            .cleanup_history_intermediate_dirs(&history_cleanup_dirs)
                            .await
                        {
                            let _ = reply.send(Err(error));
                            return;
                        }
                        self.cleanup_output_dir(output_dir.as_deref()).await;
                        self.finished_jobs.retain(|j| j.job_id != job_id);
                        let db = self.db.clone();
                        let delete_result =
                            tokio::task::spawn_blocking(move || -> Result<(), String> {
                                db.delete_job_history(job_id.0)
                                    .map_err(|e| format!("failed to delete history row: {e}"))?;
                                db.delete_job_events(job_id.0)
                                    .map_err(|e| format!("failed to delete job events: {e}"))?;
                                Ok(())
                            })
                            .await;
                        match delete_result {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                let _ = reply.send(Err(SchedulerError::Internal(error)));
                                return;
                            }
                            Err(error) => {
                                let _ = reply.send(Err(SchedulerError::Internal(format!(
                                    "failed to join history delete task: {error}"
                                ))));
                                return;
                            }
                        }
                        self.publish_snapshot();
                        Ok(())
                    }
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::DeleteAllHistory {
                delete_files,
                reply,
            } => {
                let history_cleanup_dirs = match self.all_history_cleanup_dirs().await {
                    Ok(dirs) => dirs,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                };
                if let Err(error) = self
                    .cleanup_history_intermediate_dirs(&history_cleanup_dirs)
                    .await
                {
                    let _ = reply.send(Err(error));
                    return;
                }
                if delete_files {
                    let output_dirs = self.all_output_dirs().await;
                    for dir in &output_dirs {
                        self.cleanup_output_dir(Some(dir)).await;
                    }
                }
                let terminal_job_ids: Vec<JobId> = self
                    .jobs
                    .iter()
                    .filter_map(|(job_id, state)| {
                        is_terminal_status(&state.status).then_some(*job_id)
                    })
                    .collect();
                for job_id in terminal_job_ids {
                    self.purge_terminal_job_runtime(job_id);
                }
                self.finished_jobs.clear();
                let db = self.db.clone();
                let delete_result = tokio::task::spawn_blocking(move || -> Result<(), String> {
                    db.delete_all_job_history()
                        .map_err(|e| format!("failed to delete all job history: {e}"))?;
                    db.delete_all_job_events()
                        .map_err(|e| format!("failed to delete all job events: {e}"))?;
                    Ok(())
                })
                .await;
                match delete_result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let _ = reply.send(Err(SchedulerError::Internal(error)));
                        return;
                    }
                    Err(error) => {
                        let _ = reply.send(Err(SchedulerError::Internal(format!(
                            "failed to join delete-all-history task: {error}"
                        ))));
                        return;
                    }
                }
                self.publish_snapshot();
                let _ = reply.send(Ok(()));
            }
            SchedulerCommand::Shutdown => unreachable!("handled in select"),
        }
    }
}
