use super::*;

impl Pipeline {
    pub(crate) async fn retained_nzb_path_for_job(&self, job_id: JobId) -> Option<PathBuf> {
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| matches!(state.status, JobStatus::Failed { .. }))
        {
            return Some(self.nzb_dir.join(format!("{}.nzb", job_id.0)));
        }

        let db = self.db.clone();
        let row = tokio::task::spawn_blocking(move || db.get_job_history(job_id.0))
            .await
            .ok()?
            .ok()?;
        row.and_then(|entry| entry.nzb_path).map(PathBuf::from)
    }

    pub(crate) async fn all_retained_nzb_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();

        for (job_id, state) in &self.jobs {
            if matches!(state.status, JobStatus::Failed { .. }) {
                paths.push(self.nzb_dir.join(format!("{}.nzb", job_id.0)));
            }
        }

        let db = self.db.clone();
        if let Ok(Ok(rows)) = tokio::task::spawn_blocking(move || {
            db.list_job_history(&crate::HistoryFilter::default())
        })
        .await
        {
            for row in rows {
                if let Some(path) = row.nzb_path {
                    let path = PathBuf::from(path);
                    if !paths.contains(&path) {
                        paths.push(path);
                    }
                }
            }
        }

        paths
    }

    pub(crate) async fn cleanup_retained_nzb(&self, path: Option<&std::path::Path>) {
        let Some(path) = path else { return };
        match tokio::fs::remove_file(path).await {
            Ok(()) => {
                info!(path = %path.display(), "removed retained persisted NZB");
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to remove retained persisted NZB"
                );
            }
        }
    }

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
        let row = tokio::task::spawn_blocking(move || db.get_job_history(job_id.0))
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
        let row = tokio::task::spawn_blocking(move || db.get_job_history(job_id.0))
            .await
            .ok()?
            .ok()?;
        row.and_then(|r| r.output_dir).map(PathBuf::from)
    }

    pub(crate) async fn all_output_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = Vec::new();
        for job in &self.finished_jobs {
            if let Some(dir) = &job.output_dir {
                dirs.push(PathBuf::from(dir));
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
                    if !dirs.contains(&path) {
                        dirs.push(path);
                    }
                }
            }
        }
        dirs
    }

    pub(crate) async fn cleanup_output_dir(&self, dir: Option<&std::path::Path>) {
        let Some(dir) = dir else { return };
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
        self.active_download_passes.remove(&job_id);
        self.active_downloads_by_job.remove(&job_id);
        self.job_last_download_activity.remove(&job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.clear_job_progress_floor_runtime(job_id);
        self.decode_retries
            .retain(|seg_id, _| seg_id.file_id.job_id != job_id);
        self.update_queue_metrics();
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

        let now = timestamp_secs() as i64;
        let elapsed_secs = state.created_at.elapsed().as_secs() as i64;
        let created_at = now - elapsed_secs;
        let total = state.spec.total_bytes;
        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.assembly.optional_recovery_bytes();
        let health = health_milli(total, state.failed_bytes);

        let row = crate::JobHistoryRow {
            job_id: job_id.0,
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
            nzb_path: Some(
                self.nzb_dir
                    .join(format!("{}.nzb", job_id.0))
                    .display()
                    .to_string(),
            ),
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
        self.finished_jobs.push(JobInfo {
            job_id,
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
            failed_bytes: state.failed_bytes,
            health,
            password: state.spec.password.clone(),
            category: state.spec.category.clone(),
            metadata: state.spec.metadata.clone(),
            output_dir: Some(state.working_dir.display().to_string()),
            created_at_epoch_ms: state.created_at_epoch_ms,
        });

        if let Err(e) = self.db.archive_job(job_id, &row) {
            tracing::error!(job_id = row.job_id, error = %e, "failed to archive job to history");
            return;
        }
        self.purge_terminal_job_runtime(job_id);
    }
}
