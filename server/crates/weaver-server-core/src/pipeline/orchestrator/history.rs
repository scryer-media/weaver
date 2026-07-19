use std::collections::HashSet;

use super::*;

impl Pipeline {
    fn cleanupable_history_output_dir(&self, output_dir: &std::path::Path) -> Option<PathBuf> {
        output_dir
            .strip_prefix(&self.intermediate_dir)
            .ok()
            .filter(|suffix| !suffix.as_os_str().is_empty())
            .map(|_| output_dir.to_path_buf())
    }

    pub(crate) async fn history_cleanup_dirs_for_job(
        &self,
        job_id: JobId,
    ) -> Result<BTreeSet<PathBuf>, crate::SchedulerError> {
        let mut dirs = BTreeSet::new();
        if let Some(state) = self.jobs.get(&job_id)
            && is_terminal_status(&state.status)
            && let Some(path) = self.cleanupable_history_output_dir(&state.working_dir)
        {
            dirs.insert(path);
        }

        let db = self.db.clone();
        let row = tokio::task::spawn_blocking(move || {
            db.get_job_history_profiled(
                job_id.0,
                "db.get_job_history.pipeline_history_cleanup_dirs",
            )
        })
        .await
        .map_err(|e| {
            crate::SchedulerError::Internal(format!("failed to join history lookup task: {e}"))
        })?
        .map_err(crate::SchedulerError::State)?;
        if let Some(row) = row
            && let Some(output_dir) = row.output_dir
            && let Some(path) =
                self.cleanupable_history_output_dir(std::path::Path::new(&output_dir))
        {
            dirs.insert(path);
        }

        Ok(dirs)
    }

    pub(crate) async fn all_history_cleanup_dirs(
        &self,
    ) -> Result<BTreeSet<PathBuf>, crate::SchedulerError> {
        let mut dirs = BTreeSet::new();
        for state in self.jobs.values() {
            if is_terminal_status(&state.status)
                && let Some(path) = self.cleanupable_history_output_dir(&state.working_dir)
            {
                dirs.insert(path);
            }
        }

        let db = self.db.clone();
        let rows = tokio::task::spawn_blocking(move || {
            db.list_job_history(&crate::HistoryFilter::default())
        })
        .await
        .map_err(|e| {
            crate::SchedulerError::Internal(format!("failed to join history list task: {e}"))
        })?
        .map_err(crate::SchedulerError::State)?;
        for row in rows {
            if let Some(output_dir) = row.output_dir
                && let Some(path) =
                    self.cleanupable_history_output_dir(std::path::Path::new(&output_dir))
            {
                dirs.insert(path);
            }
        }

        Ok(dirs)
    }

    pub(crate) async fn cleanup_history_intermediate_dirs(
        &self,
        dirs: &BTreeSet<PathBuf>,
    ) -> Result<(), crate::SchedulerError> {
        for dir in dirs {
            // Failed jobs never pass through the finalize close, so drop any
            // cached write handles before their dirs (and paths) are freed
            // for reuse.
            crate::pipeline::close_cached_write_handles_under(dir).await;
            match tokio::fs::remove_dir_all(dir).await {
                Ok(()) => {
                    info!(dir = %dir.display(), "removed historical intermediate directory");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(crate::SchedulerError::Io(std::io::Error::new(
                        error.kind(),
                        format!(
                            "failed to remove historical intermediate directory '{}': {error}",
                            dir.display()
                        ),
                    )));
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn output_dir_for_job(&self, job_id: JobId) -> Option<PathBuf> {
        if let Some(dir) = self
            .finished_jobs
            .iter()
            .find(|j| j.job_id == job_id)
            .and_then(|j| j.output_dir.as_ref())
        {
            return Some(PathBuf::from(dir));
        }
        if let Some(state) = self.jobs.get(&job_id) {
            return Some(state.working_dir.clone());
        }
        let db = self.db.clone();
        let row = tokio::task::spawn_blocking(move || {
            db.get_job_history_profiled(job_id.0, "db.get_job_history.pipeline_output_dir_for_job")
        })
        .await
        .ok()?
        .ok()?;
        row.and_then(|r| r.output_dir).map(PathBuf::from)
    }

    pub(crate) async fn all_output_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = Vec::new();
        let mut seen = HashSet::new();
        for job in &self.finished_jobs {
            if let Some(dir) = &job.output_dir {
                let path = PathBuf::from(dir);
                if seen.insert(path.clone()) {
                    dirs.push(path);
                }
            }
        }
        let db = self.db.clone();
        if let Ok(Ok(rows)) = tokio::task::spawn_blocking(move || {
            db.list_job_history(&crate::HistoryFilter::default())
        })
        .await
        {
            for row in rows {
                if let Some(dir) = row.output_dir {
                    let path = PathBuf::from(&dir);
                    if seen.insert(path.clone()) {
                        dirs.push(path);
                    }
                }
            }
        }
        dirs
    }

    pub(crate) async fn cleanup_output_dir(&self, dir: Option<&std::path::Path>) {
        let Some(dir) = dir else { return };
        match tokio::fs::symlink_metadata(dir).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => {
                warn!(dir = %dir.display(), error = %error, "could not inspect complete output directory before cleanup");
                return;
            }
            Ok(_) => {}
        }
        let ownership_path = dir.to_path_buf();
        let owned = tokio::task::spawn_blocking(move || {
            crate::jobs::working_dir::is_weaver_owned_output_dir(&ownership_path)
        })
        .await
        .unwrap_or(false);
        if !owned {
            warn!(
                dir = %dir.display(),
                "refusing recursive cleanup of an output directory without a valid Weaver ownership marker"
            );
            return;
        }
        match tokio::fs::remove_dir_all(dir).await {
            Ok(()) => {
                info!(dir = %dir.display(), "removed complete output directory");
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(
                    dir = %dir.display(),
                    error = %error,
                    "failed to remove complete output directory"
                );
            }
        }
    }

    pub(crate) fn purge_terminal_job_runtime(&mut self, job_id: JobId) {
        self.jobs.remove(&job_id);
        self.job_order.retain(|id| *id != job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_extraction_runtime(job_id);
        self.post_processing_repair_reentered.remove(&job_id);
        self.post_processing_repair_return_to_terminal
            .remove(&job_id);
        self.inflight_moves.remove(&job_id);
        self.reserved_complete_destinations.remove(&job_id);
        self.active_download_passes.remove(&job_id);
        self.jobs_finalizing_download.remove(&job_id);
        self.active_downloads_by_job.remove(&job_id);
        self.active_download_connections_by_job.remove(&job_id);
        self.job_last_download_activity.remove(&job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.clear_job_progress_floor_runtime(job_id);
        self.clear_job_phase_progress_runtime(job_id);
        self.clear_job_retention_excludes(job_id);
        self.decode_retries
            .retain(|seg_id, _| seg_id.file_id.job_id != job_id);
        self.rate_limit_reservations
            .retain(|seg_id, _| seg_id.file_id.job_id != job_id);
        self.update_queue_metrics();
    }

    pub(crate) fn enforce_finished_jobs_runtime_cap(&mut self) {
        self.finished_jobs
            .truncate(crate::jobs::FINISHED_JOBS_RUNTIME_CAP);
    }

    pub(crate) fn record_job_history(&mut self, job_id: JobId) {
        let state = match self.jobs.get(&job_id) {
            Some(s) => s,
            None => return,
        };

        let (status_str, error_message) = match &state.status {
            JobStatus::Complete => ("complete".to_string(), None),
            JobStatus::Failed { error } => ("failed".to_string(), Some(error.clone())),
            _ => return,
        };
        let completed = matches!(state.status, JobStatus::Complete);

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
            status: status_str,
            error_message,
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

        self.finished_jobs.retain(|j| j.job_id != job_id);
        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.assembly.optional_recovery_bytes();
        let (download_state, post_state, run_state) =
            crate::jobs::model::runtime_lanes_from_status_snapshot(&state.status);
        self.finished_jobs.insert(
            0,
            JobInfo {
                job_id,
                job_hash: Some(state.job_hash),
                name: state.spec.name.clone(),
                error: if let JobStatus::Failed { error } = &state.status {
                    Some(error.clone())
                } else {
                    None
                },
                status: state.status.clone(),
                download_state,
                post_state,
                run_state,
                progress: Self::effective_progress(state),
                total_bytes: total,
                downloaded_bytes: Self::effective_downloaded_bytes(state),
                optional_recovery_bytes,
                optional_recovery_downloaded_bytes,
                phase_progress: Vec::new(),
                failed_bytes: state.failed_bytes,
                health,
                total_files: state.assembly.total_file_count() as u32,
                completed_files: state.assembly.complete_file_count() as u32,
                remaining_par_files: 0,
                password: state.spec.password.clone(),
                category: state.spec.category.clone(),
                metadata: state.spec.metadata.clone(),
                output_dir: Some(state.working_dir.display().to_string()),
                created_at_epoch_ms: state.created_at_epoch_ms,
            },
        );
        self.enforce_finished_jobs_runtime_cap();

        let typed_terminal_cause = if completed {
            Some(crate::jobs::SemanticTerminalCause::Success)
        } else {
            self.semantic_terminal_causes.remove(&job_id)
        };

        let archive_started = Instant::now();
        if let Err(e) =
            self.db
                .try_queue_archive_job_with_terminal_cause(job_id, row, typed_terminal_cause)
        {
            tracing::error!(job_id = job_id.0, error = %e, "failed to queue job history archival");
            return;
        }
        debug!(
            job_id = job_id.0,
            elapsed_ms = archive_started.elapsed().as_millis(),
            "queued job history archival"
        );
        self.purge_terminal_job_runtime(job_id);
    }
}
