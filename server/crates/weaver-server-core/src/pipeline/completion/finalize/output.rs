use super::*;
use std::path::PathBuf;

use crate::jobs::working_dir::WORKING_DIR_MARKER;

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
    std::fs::copy(src, dst)?;
    std::fs::remove_file(src)?;
    Ok(())
}

impl Pipeline {
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

        if !base_dest.exists() {
            return base_dest;
        }

        let parent = base_dest
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let suffixed = parent.join(format!("{}.#{}", dir_name, job_id.0));
        if !suffixed.exists() {
            return suffixed;
        }

        let mut attempt = 1u32;
        loop {
            let candidate = parent.join(format!("{}.#{}.{}", dir_name, job_id.0, attempt));
            if !candidate.exists() {
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
    pub(crate) async fn move_to_complete(&mut self, job_id: JobId) -> Result<PathBuf, String> {
        let (working_dir, staging_dir, job_name, category) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Err(format!("job {} not found for move_to_complete", job_id.0));
            };
            (
                state.working_dir.clone(),
                state.staging_dir.clone(),
                state.spec.name.clone(),
                state.spec.category.clone(),
            )
        };

        self.transition_postprocessing_status(job_id, JobStatus::Moving, Some("moving"));

        let _ = self
            .event_tx
            .send(PipelineEvent::MoveToCompleteStarted { job_id });

        let dest = self
            .compute_complete_destination(job_id, &job_name, category.as_deref())
            .await;

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

        // Ensure destination exists.
        if let Err(e) = tokio::fs::create_dir_all(&dest).await {
            return Err(format!(
                "failed to create complete directory {}: {e}",
                dest.display()
            ));
        }

        let mut moved = 0u32;
        let mut failures = Vec::new();

        // Move extracted files from the staging directory (same filesystem as
        // complete_dir, so renames are instant).
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

        // Move any remaining non-archive files from working_dir (NFOs, SRTs, etc.).
        if let Ok(mut entries) = tokio::fs::read_dir(&working_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let file_name = entry.file_name();
                // Skip leftover chunk workspaces and staging dirs.
                if file_name == ".weaver-chunks"
                    || file_name == ".weaver-staging"
                    || file_name == WORKING_DIR_MARKER
                {
                    continue;
                }
                let src = entry.path();
                let dst = dest.join(&file_name);
                // Skip if the destination already has this file (from staging).
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
                "failed to move {} entr{} to complete directory",
                failures.len(),
                if failures.len() == 1 { "y" } else { "ies" }
            ));
        }

        // Remove the now-empty staging directory.
        if let Some(ref staging) = staging_dir
            && let Err(e) = tokio::fs::remove_dir_all(staging).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                job_id = job_id.0,
                dir = %staging.display(),
                error = %e,
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

        // Remove the now-empty intermediate directory.
        if let Err(e) = tokio::fs::remove_dir(&working_dir).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                job_id = job_id.0,
                dir = %working_dir.display(),
                error = %e,
                "failed to remove intermediate directory after move"
            );
        }

        // Update working_dir to point to the complete path (for API reporting).
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.working_dir = dest.clone();
            state.staging_dir = None;
        }

        info!(
            job_id = job_id.0,
            moved,
            dest = %dest.display(),
            "moved files to complete directory"
        );
        let _ = self
            .event_tx
            .send(PipelineEvent::MoveToCompleteFinished { job_id });
        Ok(dest)
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
