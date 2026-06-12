use super::*;
use std::path::PathBuf;

use crate::jobs::working_dir::WORKING_DIR_MARKER;
use crate::runtime::file_cache;

fn move_path_with_copy_fallback(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(src)?;

    if metadata.is_dir() {
        if dst.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dst.display()),
            ));
        }

        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            move_path_with_copy_fallback(&entry.path(), &dst.join(entry.file_name()))?;
        }
        std::fs::remove_dir(src)?;
        return Ok(());
    }

    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    file_cache::copy_large_file(src, dst)?;
    std::fs::remove_file(src)?;
    Ok(())
}

async fn run_move_to_complete(
    job_id: JobId,
    working_dir: PathBuf,
    staging_dir: Option<PathBuf>,
    dest: PathBuf,
) -> Result<MoveToCompleteResult, String> {
    // Verify at least one source directory exists before creating
    // the destination, so a missing source doesn't leave behind an
    // empty complete dir.
    let staging_exists = staging_dir.as_ref().is_some_and(|s| s.exists());
    let working_exists = working_dir.exists();
    if !staging_exists && !working_exists {
        return Err(format!(
            "failed to read working directory {} for move: No such file or directory (os error 2)",
            working_dir.display()
        ));
    }

    if let Err(error) = tokio::fs::create_dir_all(&dest).await {
        return Err(format!(
            "failed to create complete directory {}: {error}",
            dest.display()
        ));
    }

    let mut moved = 0u32;
    let mut failures = Vec::new();

    if let Some(ref staging) = staging_dir
        && let Ok(mut entries) = tokio::fs::read_dir(staging).await
    {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name();
            if file_name == ".weaver-chunks" {
                let src = entry.path();
                if let Err(error) = tokio::fs::remove_dir_all(&src).await
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(
                        job_id = job_id.0,
                        path = %src.display(),
                        error = %error,
                        "failed to remove chunk workspace during final move"
                    );
                }
                continue;
            }
            let src = entry.path();
            let dst = dest.join(&file_name);
            match tokio::fs::rename(&src, &dst).await {
                Ok(()) => {
                    moved += 1;
                }
                Err(rename_err) => {
                    let src_fb = src.clone();
                    let dst_fb = dst.clone();
                    match tokio::task::spawn_blocking(move || {
                        move_path_with_copy_fallback(&src_fb, &dst_fb)
                    })
                    .await
                    {
                        Ok(Ok(())) => {
                            moved += 1;
                        }
                        Ok(Err(copy_err)) => {
                            failures.push(format!(
                                "{}: rename failed: {}; fallback failed: {}",
                                file_name.to_string_lossy(),
                                rename_err,
                                copy_err
                            ));
                        }
                        Err(join_err) => {
                            failures.push(format!(
                                "{}: rename failed: {}; fallback task failed: {}",
                                file_name.to_string_lossy(),
                                rename_err,
                                join_err
                            ));
                        }
                    }
                }
            }
        }
    }

    if let Ok(mut entries) = tokio::fs::read_dir(&working_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name();
            if file_name == ".weaver-chunks"
                || file_name == ".weaver-staging"
                || file_name == WORKING_DIR_MARKER
            {
                continue;
            }
            let src = entry.path();
            let dst = dest.join(&file_name);
            if dst.exists() {
                continue;
            }
            match tokio::fs::rename(&src, &dst).await {
                Ok(()) => {
                    moved += 1;
                }
                Err(rename_err) => {
                    let src_fb = src.clone();
                    let dst_fb = dst.clone();
                    match tokio::task::spawn_blocking(move || {
                        move_path_with_copy_fallback(&src_fb, &dst_fb)
                    })
                    .await
                    {
                        Ok(Ok(())) => {
                            moved += 1;
                        }
                        Ok(Err(copy_err)) => {
                            failures.push(format!(
                                "{}: rename failed: {}; fallback failed: {}",
                                file_name.to_string_lossy(),
                                rename_err,
                                copy_err
                            ));
                        }
                        Err(join_err) => {
                            failures.push(format!(
                                "{}: rename failed: {}; fallback task failed: {}",
                                file_name.to_string_lossy(),
                                rename_err,
                                join_err
                            ));
                        }
                    }
                }
            }
        }
    }

    if !failures.is_empty() {
        for failure in &failures {
            warn!(job_id = job_id.0, error = %failure, "failed to move entry to complete directory");
        }
        return Err(format!(
            "failed to move {} entr{} to complete directory: {}",
            failures.len(),
            if failures.len() == 1 { "y" } else { "ies" },
            failures[0]
        ));
    }

    if let Some(ref staging) = staging_dir
        && let Err(error) = tokio::fs::remove_dir_all(staging).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            job_id = job_id.0,
            dir = %staging.display(),
            error = %error,
            "failed to remove staging directory after move"
        );
    }

    let marker_path = working_dir.join(WORKING_DIR_MARKER);
    if let Err(error) = tokio::fs::remove_file(&marker_path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            job_id = job_id.0,
            path = %marker_path.display(),
            error = %error,
            "failed to remove working directory marker during final move"
        );
    }

    if let Err(error) = tokio::fs::remove_dir(&working_dir).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            job_id = job_id.0,
            dir = %working_dir.display(),
            error = %error,
            "failed to remove intermediate directory after move"
        );
    }

    Ok(MoveToCompleteResult {
        moved_entries: moved,
    })
}

impl Pipeline {
    fn complete_destination_is_reserved(&self, job_id: JobId, candidate: &std::path::Path) -> bool {
        self.reserved_complete_destinations
            .iter()
            .any(|(reserved_job_id, reserved_path)| {
                *reserved_job_id != job_id && reserved_path == candidate
            })
    }

    async fn compute_complete_destination(
        &self,
        job_id: JobId,
        job_name: &str,
        category: Option<&str>,
    ) -> PathBuf {
        let dir_name = crate::jobs::working_dir::sanitize_dirname(job_name);
        let base_dest = {
            let cfg = self.config.read().await;
            let cat_dest = category.and_then(|cat| {
                cfg.categories
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(cat))
                    .and_then(|c| c.dest_dir.as_ref())
                    .filter(|d| !d.is_empty())
                    .map(PathBuf::from)
            });

            if let Some(custom_dest) = cat_dest {
                custom_dest.join(&dir_name)
            } else {
                let mut dest = self.complete_dir.clone();
                if let Some(cat) = category
                    && !cat.is_empty()
                {
                    dest = dest.join(cat);
                }
                dest.join(&dir_name)
            }
        };

        if !base_dest.exists() && !self.complete_destination_is_reserved(job_id, &base_dest) {
            return base_dest;
        }

        let parent = base_dest
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let suffixed = parent.join(format!("{}.#{}", dir_name, job_id.0));
        if !suffixed.exists() && !self.complete_destination_is_reserved(job_id, &suffixed) {
            return suffixed;
        }

        let mut attempt = 1u32;
        loop {
            let candidate = parent.join(format!("{}.#{}.{}", dir_name, job_id.0, attempt));
            if !candidate.exists() && !self.complete_destination_is_reserved(job_id, &candidate) {
                return candidate;
            }
            attempt += 1;
        }
    }

    /// Move extracted/completed files from the intermediate working directory
    /// to the complete directory, organized by category.
    ///
    /// Layout: `{complete_dir}/[{category}/]{job_name}/`
    /// On collision, appends `.#<job_id>` (and a numeric suffix if needed).
    ///
    /// Uses rename() for same-filesystem moves, falls back to copy+delete for cross-FS.
    pub(crate) async fn start_move_to_complete(&mut self, job_id: JobId) -> Result<(), String> {
        if self.inflight_moves.contains(&job_id) {
            return Ok(());
        }

        let (working_dir, staging_dir, job_name, category) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Err(format!("job {} not found for final move", job_id.0));
            };
            (
                state.working_dir.clone(),
                state.staging_dir.clone(),
                state.spec.name.clone(),
                state.spec.category.clone(),
            )
        };

        self.transition_postprocessing_status(job_id, JobStatus::Moving, Some("moving"));

        let dest = self
            .compute_complete_destination(job_id, &job_name, category.as_deref())
            .await;
        self.reserved_complete_destinations
            .insert(job_id, dest.clone());
        self.inflight_moves.insert(job_id);

        let _ = self
            .event_tx
            .send(PipelineEvent::MoveToCompleteStarted { job_id });
        self.publish_snapshot();

        let move_done_tx = self.move_done_tx.clone();
        info!(
            job_id = job_id.0,
            dest = %dest.display(),
            "starting final move"
        );
        tokio::spawn(async move {
            let move_started = Instant::now();
            let result = run_move_to_complete(job_id, working_dir, staging_dir, dest.clone()).await;
            match &result {
                Ok(outcome) => info!(
                    job_id = job_id.0,
                    moved = outcome.moved_entries,
                    dest = %dest.display(),
                    elapsed_ms = move_started.elapsed().as_millis(),
                    "final move finished"
                ),
                Err(error) => warn!(
                    job_id = job_id.0,
                    dest = %dest.display(),
                    elapsed_ms = move_started.elapsed().as_millis(),
                    error = %error,
                    "final move failed"
                ),
            }
            let _ = move_done_tx
                .send(MoveToCompleteDone {
                    job_id,
                    dest,
                    result,
                })
                .await;
        });
        Ok(())
    }

    pub(crate) fn handle_move_to_complete_done(&mut self, done: MoveToCompleteDone) {
        let MoveToCompleteDone {
            job_id,
            dest,
            result,
        } = done;
        self.inflight_moves.remove(&job_id);
        self.reserved_complete_destinations.remove(&job_id);

        match result {
            Ok(outcome) => {
                let Some(state) = self.jobs.get_mut(&job_id) else {
                    warn!(
                        job_id = job_id.0,
                        dest = %dest.display(),
                        "final move finished after job runtime was removed"
                    );
                    return;
                };
                state.working_dir = dest.clone();
                state.staging_dir = None;

                let _ = self
                    .event_tx
                    .send(PipelineEvent::MoveToCompleteFinished { job_id });
                self.transition_completed_runtime(job_id);
                if self.active_download_passes.remove(&job_id) {
                    let _ = self.event_tx.send(PipelineEvent::DownloadFinished {
                        job_id,
                        finalization_pending: false,
                    });
                }
                self.jobs_finalizing_download.remove(&job_id);
                self.clear_par2_runtime_state(job_id);
                self.clear_job_rar_runtime(job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                info!(
                    job_id = job_id.0,
                    moved = outcome.moved_entries,
                    dest = %dest.display(),
                    "job completed after final move"
                );
                self.record_job_history(job_id);
                self.publish_snapshot();
            }
            Err(error) => self.fail_job(job_id, error),
        }
    }

    pub(in crate::pipeline) fn resolve_job_input_path(
        &self,
        job_id: JobId,
        relative_path: &str,
    ) -> Option<PathBuf> {
        let state = self.jobs.get(&job_id)?;
        let working_path = state.working_dir.join(relative_path);
        if working_path.exists() {
            return Some(working_path);
        }

        if let Some(staging_dir) = state.staging_dir.as_ref() {
            let staging_path = staging_dir.join(relative_path);
            if staging_path.exists() {
                return Some(staging_path);
            }
        }

        Some(working_path)
    }
}

#[cfg(test)]
mod tests;
