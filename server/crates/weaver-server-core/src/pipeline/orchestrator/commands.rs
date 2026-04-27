use super::*;

impl Pipeline {
    pub(crate) async fn handle_command(&mut self, cmd: SchedulerCommand) {
        match cmd {
            SchedulerCommand::AddJob {
                job_id,
                spec,
                nzb_path,
                reply,
            } => {
                let result = self.add_job(job_id, spec, nzb_path).await;
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
            SchedulerCommand::CancelJob { job_id, reply } => {
                let result = if let Some(state) = self.jobs.remove(&job_id) {
                    self.job_order.retain(|id| *id != job_id);
                    self.remove_pending_completion_check(job_id);
                    self.update_queue_metrics();

                    let now = timestamp_secs() as i64;
                    let elapsed_secs = state.created_at.elapsed().as_secs() as i64;
                    let created_at = now - elapsed_secs;
                    let total = state.spec.total_bytes;
                    let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
                        state.assembly.optional_recovery_bytes();
                    let health = if total == 0 {
                        1000
                    } else {
                        ((total.saturating_sub(state.failed_bytes)) * 1000 / total) as u32
                    };
                    let row = crate::JobHistoryRow {
                        job_id: job_id.0,
                        name: state.spec.name.clone(),
                        status: "cancelled".to_string(),
                        error_message: None,
                        total_bytes: total,
                        downloaded_bytes: Self::effective_downloaded_bytes(&state),
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
                            let nzb_path = self.nzb_dir.join(format!("{}.nzb", job_id.0));
                            move |db| {
                                db.archive_job(job_id, &row).map_err(|e| {
                                    format!("failed to archive cancelled job: {e}")
                                })?;
                                if let Err(e) = std::fs::remove_file(&nzb_path)
                                    && e.kind() != std::io::ErrorKind::NotFound
                                {
                                    tracing::warn!(path = %nzb_path.display(), error = %e, "failed to remove NZB file");
                                }
                                Ok::<(), String>(())
                            }
                        })
                        .await;
                    if let Err(error) = archive_result {
                        tracing::error!(job_id = job_id.0, error = %error, "failed to durably archive cancelled job");
                    }

                    self.clear_par2_runtime_state(job_id);
                    self.clear_job_extraction_runtime(job_id);
                    self.active_download_passes.remove(&job_id);
                    self.active_downloads_by_job.remove(&job_id);
                    self.job_last_download_activity.remove(&job_id);
                    self.clear_job_rar_runtime(job_id);
                    self.clear_job_write_backlog(job_id);
                    self.clear_job_progress_floor_runtime(job_id);

                    let working_dir = state.working_dir.clone();
                    let staging_dir = state.staging_dir.clone();
                    tokio::spawn(async move {
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
                    self.db_fire_and_forget(move |db| {
                        if let Err(e) = db.update_active_job(job_id, &update) {
                            error!(error = %e, "db write failed for UpdateJob");
                        }
                    });
                    self.publish_snapshot();
                    Ok(())
                } else {
                    Err(crate::SchedulerError::JobNotFound(job_id))
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
                if let Ok(new_client) = client.downcast::<NntpClient>() {
                    self.nntp = Arc::new(*new_client);
                    self.connection_ramp = total_connections.min(5);
                    self.tuner.set_connection_limit(total_connections);
                    info!(
                        total_connections,
                        max_downloads = self.tuner.params().max_concurrent_downloads,
                        "NNTP client rebuilt with new server config"
                    );
                }
                let _ = reply.send(());
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
                        let _ = std::fs::create_dir_all(&self.nzb_dir);
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
                let retained_nzb_path = self.retained_nzb_path_for_job(job_id).await;
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
                        self.cleanup_retained_nzb(retained_nzb_path.as_deref())
                            .await;
                        self.purge_terminal_job_runtime(job_id);
                        self.finished_jobs.retain(|j| j.job_id != job_id);
                        let db = self.db.clone();
                        tokio::task::spawn_blocking(move || {
                            let _ = db.delete_job_history(job_id.0);
                            let _ = db.delete_job_events(job_id.0);
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
                        self.cleanup_retained_nzb(retained_nzb_path.as_deref())
                            .await;
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
                let retained_nzb_paths = self.all_retained_nzb_paths().await;
                for path in &retained_nzb_paths {
                    self.cleanup_retained_nzb(Some(path)).await;
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
