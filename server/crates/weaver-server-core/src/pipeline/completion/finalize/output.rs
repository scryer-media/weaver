use super::*;
use std::path::PathBuf;

use crate::jobs::working_dir::{WORKING_DIR_MARKER, mark_weaver_owned_output_dir};
use crate::runtime::{file_cache, fs as runtime_fs};

fn move_path_with_copy_fallback(
    src: &std::path::Path,
    dst: &std::path::Path,
    phase_counters: &PhaseCounters,
) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(src)?;

    if metadata.file_type().is_symlink() {
        runtime_fs::rename_no_overwrite(src, dst)?;
        return Ok(());
    }

    if metadata.is_dir() {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir(dst).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("destination already exists: {}", dst.display()),
                )
            } else {
                error
            }
        })?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            move_path_with_copy_fallback(
                &entry.path(),
                &dst.join(entry.file_name()),
                phase_counters,
            )?;
        }
        std::fs::remove_dir(src)?;
        return Ok(());
    }

    let parent_fingerprint = runtime_fs::prepare_destination_parent(dst)?;
    match file_cache::copy_large_file_with_progress(src, dst, |copied| {
        phase_counters
            .completed_bytes
            .fetch_add(copied, Ordering::Relaxed);
    }) {
        Ok(_) => {}
        Err(error) => {
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                cleanup_copy_destination_if_parent_matches(dst, &parent_fingerprint);
            }
            return Err(error);
        }
    }
    if !runtime_fs::destination_parent_matches(dst, &parent_fingerprint)? {
        return Err(std::io::Error::other(format!(
            "destination parent changed during copy: {}",
            dst.display()
        )));
    }
    std::fs::remove_file(src)?;
    Ok(())
}

fn cleanup_copy_destination_if_parent_matches(
    dst: &std::path::Path,
    parent_fingerprint: &runtime_fs::DirectoryFingerprint,
) {
    if runtime_fs::destination_parent_matches(dst, parent_fingerprint).unwrap_or(false) {
        let _ = std::fs::remove_file(dst);
    }
}

fn move_path_with_safe_rename_or_copy_fallback(
    src: &std::path::Path,
    dst: &std::path::Path,
    phase_counters: Arc<PhaseCounters>,
) -> Result<(), (std::io::Error, std::io::Error)> {
    let path_bytes = path_regular_file_bytes(src).unwrap_or(0);
    match runtime_fs::rename_no_overwrite(src, dst) {
        Ok(()) => {
            if path_bytes > 0 {
                phase_counters
                    .completed_bytes
                    .fetch_add(path_bytes, Ordering::Relaxed);
            }
            Ok(())
        }
        Err(rename_err) => move_path_with_copy_fallback(src, dst, &phase_counters)
            .map_err(|copy_err| (rename_err, copy_err)),
    }
}

fn path_regular_file_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }
    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(path_regular_file_bytes(&entry.path())?);
    }
    Ok(total)
}

async fn run_move_to_complete(
    job_id: JobId,
    working_dir: PathBuf,
    staging_dir: Option<PathBuf>,
    dest: PathBuf,
    phase_counters: Arc<PhaseCounters>,
) -> Result<MoveToCompleteResult, String> {
    // Every cached disk write handle under the working dir must be closed
    // before its files are renamed or moved.
    crate::pipeline::close_cached_write_handles_under(&working_dir).await;

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

    let total_bytes = {
        let working_dir = working_dir.clone();
        let staging_dir = staging_dir.clone();
        tokio::task::spawn_blocking(move || {
            move_sources_regular_file_bytes(&working_dir, staging_dir.as_deref())
        })
        .await
        .unwrap_or(0)
    };
    phase_counters
        .total_bytes
        .store(total_bytes, Ordering::Relaxed);

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
            let src_fb = src.clone();
            let dst_fb = dst.clone();
            let counters = Arc::clone(&phase_counters);
            match tokio::task::spawn_blocking(move || {
                move_path_with_safe_rename_or_copy_fallback(&src_fb, &dst_fb, counters)
            })
            .await
            {
                Ok(Ok(())) => {
                    moved += 1;
                }
                Ok(Err((rename_err, copy_err))) => {
                    failures.push(format!(
                        "{}: rename failed: {}; fallback failed: {}",
                        file_name.to_string_lossy(),
                        rename_err,
                        copy_err
                    ));
                }
                Err(join_err) => {
                    failures.push(format!(
                        "{}: move task failed: {}",
                        file_name.to_string_lossy(),
                        join_err
                    ));
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
            if dst.exists() && !runtime_fs::paths_equivalent_for_placement(&src, &dst) {
                continue;
            }
            let src_fb = src.clone();
            let dst_fb = dst.clone();
            let counters = Arc::clone(&phase_counters);
            match tokio::task::spawn_blocking(move || {
                move_path_with_safe_rename_or_copy_fallback(&src_fb, &dst_fb, counters)
            })
            .await
            {
                Ok(Ok(())) => {
                    moved += 1;
                }
                Ok(Err((rename_err, copy_err))) => {
                    failures.push(format!(
                        "{}: rename failed: {}; fallback failed: {}",
                        file_name.to_string_lossy(),
                        rename_err,
                        copy_err
                    ));
                }
                Err(join_err) => {
                    failures.push(format!(
                        "{}: move task failed: {}",
                        file_name.to_string_lossy(),
                        join_err
                    ));
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

    let output_dir = dest.clone();
    match tokio::task::spawn_blocking(move || mark_weaver_owned_output_dir(&output_dir)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(
            job_id = job_id.0,
            dir = %dest.display(),
            error = %error,
            "could not mark complete output directory as Weaver-owned; recursive history cleanup will refuse it"
        ),
        Err(error) => warn!(
            job_id = job_id.0,
            dir = %dest.display(),
            error = %error,
            "output ownership marker worker failed; recursive history cleanup will refuse the directory"
        ),
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

fn move_sources_regular_file_bytes(
    working_dir: &std::path::Path,
    staging_dir: Option<&std::path::Path>,
) -> u64 {
    let mut total = 0u64;
    if let Some(staging) = staging_dir
        && let Ok(entries) = std::fs::read_dir(staging)
    {
        for entry in entries.flatten() {
            if entry.file_name() == ".weaver-chunks" {
                continue;
            }
            total = total.saturating_add(path_regular_file_bytes(&entry.path()).unwrap_or(0));
        }
    }
    if let Ok(entries) = std::fs::read_dir(working_dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            if file_name == ".weaver-chunks"
                || file_name == ".weaver-staging"
                || file_name == WORKING_DIR_MARKER
            {
                continue;
            }
            total = total.saturating_add(path_regular_file_bytes(&entry.path()).unwrap_or(0));
        }
    }
    total
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
        let suffixed = parent.join(weaver_model::files::path_component_with_suffix(
            &dir_name,
            &format!(".#{}", job_id.0),
        ));
        if !suffixed.exists() && !self.complete_destination_is_reserved(job_id, &suffixed) {
            return suffixed;
        }

        let mut attempt = 1u32;
        loop {
            let candidate = parent.join(weaver_model::files::path_component_with_suffix(
                &dir_name,
                &format!(".#{}.{}", job_id.0, attempt),
            ));
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

        if self
            .post_processing_repair_return_to_terminal
            .remove(&job_id)
        {
            self.phase_end(job_id, JobPhase::Extracting);
            self.phase_end(job_id, JobPhase::Repairing);
            info!(
                job_id = job_id.0,
                "requested PAR pass finished; resuming terminal post-processing in final directory"
            );
            let plan = self
                .db_blocking(move |db| {
                    if !db.post_processing_settings()?.execution_enabled {
                        return Ok(None);
                    }
                    db.frozen_post_processing_plan(job_id.0)
                })
                .await
                .map_err(|error| format!("failed to load frozen post-processing plan: {error}"))?;
            if let Some(plan) = plan.filter(|plan| !plan.steps().is_empty()) {
                self.start_terminal_post_processing(job_id, plan);
            } else {
                self.complete_job_after_terminal_post_processing(job_id);
            }
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

        self.phase_end(job_id, JobPhase::Extracting);
        self.phase_end(job_id, JobPhase::Repairing);
        let phase_counters = self.phase_begin(job_id, JobPhase::Moving, None);
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
            let result = run_move_to_complete(
                job_id,
                working_dir,
                staging_dir,
                dest.clone(),
                phase_counters,
            )
            .await;
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

    pub(crate) async fn handle_move_to_complete_done(&mut self, done: MoveToCompleteDone) {
        let MoveToCompleteDone {
            job_id,
            dest,
            result,
        } = done;
        self.phase_end(job_id, JobPhase::Moving);
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
                info!(
                    job_id = job_id.0,
                    moved = outcome.moved_entries,
                    dest = %dest.display(),
                    "built-in pipeline completed final move"
                );
                let plan = self
                    .db_blocking(move |db| {
                        if !db.post_processing_settings()?.execution_enabled {
                            return Ok(None);
                        }
                        db.frozen_post_processing_plan(job_id.0)
                    })
                    .await;
                match plan {
                    Ok(Some(plan)) if !plan.steps().is_empty() => {
                        self.start_terminal_post_processing(job_id, plan);
                    }
                    Ok(_) => self.complete_job_after_terminal_post_processing(job_id),
                    Err(error) => self.fail_job(
                        job_id,
                        format!("failed to load frozen post-processing plan: {error}"),
                    ),
                }
            }
            Err(error) => self.fail_job(job_id, error),
        }
    }

    fn start_terminal_post_processing(
        &mut self,
        job_id: JobId,
        plan: crate::post_processing::model::FrozenPlan,
    ) {
        self.start_terminal_post_processing_with_outcome(
            job_id,
            plan,
            crate::post_processing::model::PipelineOutcome::Succeeded,
            crate::post_processing::persistence::TerminalIntent::Complete,
            None,
        );
    }

    pub(crate) fn start_terminal_post_processing_with_outcome(
        &mut self,
        job_id: JobId,
        plan: crate::post_processing::model::FrozenPlan,
        pipeline_outcome: crate::post_processing::model::PipelineOutcome,
        terminal_intent: crate::post_processing::persistence::TerminalIntent,
        primary_failure: Option<String>,
    ) {
        if !self.jobs.contains_key(&job_id) {
            return;
        }
        if !self.inflight_terminal_post_processing.insert(job_id) {
            return;
        }
        let run_id = match self.db.create_post_processing_run(
            job_id.0,
            &plan,
            &pipeline_outcome,
            terminal_intent,
            None,
            chrono::Utc::now().timestamp_millis(),
        ) {
            Ok(run_id) => run_id,
            Err(error) => {
                self.inflight_terminal_post_processing.remove(&job_id);
                let error = format!("failed to persist terminal post-processing run: {error}");
                self.finalize_failed_job_after_terminal_post_processing(
                    job_id,
                    primary_failure.unwrap_or(error),
                );
                return;
            }
        };
        self.launch_terminal_post_processing_run(job_id, run_id, pipeline_outcome, primary_failure);
    }

    fn launch_terminal_post_processing_run(
        &mut self,
        job_id: JobId,
        run_id: crate::post_processing::model::RunId,
        pipeline_outcome: crate::post_processing::model::PipelineOutcome,
        primary_failure: Option<String>,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            self.inflight_terminal_post_processing.remove(&job_id);
            let _ = self.db.finish_post_processing_run(
                &run_id,
                crate::post_processing::model::RunStatus::Interrupted,
                crate::post_processing::model::PostProcessingSummary::Interrupted,
                chrono::Utc::now().timestamp_millis(),
            );
            return;
        };
        let settings = self.db.post_processing_settings().unwrap_or_default();
        let interpreters = crate::post_processing::runner::InterpreterConfig {
            python: settings.python_interpreter.map(std::path::PathBuf::from),
            powershell: settings
                .powershell_interpreter
                .map(std::path::PathBuf::from),
            batch: settings.batch_interpreter.map(std::path::PathBuf::from),
        };
        let pipeline_failure_stage = match &pipeline_outcome {
            crate::post_processing::model::PipelineOutcome::Failed { stage, .. } => Some(*stage),
            crate::post_processing::model::PipelineOutcome::Succeeded => None,
        };
        let par_status = if matches!(
            pipeline_failure_stage,
            Some(
                crate::post_processing::model::PipelineFailureStage::Verify
                    | crate::post_processing::model::PipelineFailureStage::Repair
            )
        ) {
            1
        } else if self.par2_verified.contains(&job_id) {
            2
        } else {
            0
        };
        let unpack_status = if matches!(
            pipeline_failure_stage,
            Some(crate::post_processing::model::PipelineFailureStage::Extract)
        ) {
            1
        } else if self
            .extracted_archives
            .get(&job_id)
            .is_some_and(|archives| !archives.is_empty())
        {
            2
        } else {
            0
        };
        let (data_dir, intermediate_dir, complete_dir) = self
            .config
            .try_read()
            .ok()
            .map(|config| {
                (
                    std::path::PathBuf::from(&config.data_dir),
                    std::path::PathBuf::from(config.intermediate_dir()),
                    std::path::PathBuf::from(config.complete_dir()),
                )
            })
            .unwrap_or_else(|| {
                (
                    self.intermediate_dir
                        .parent()
                        .map(std::path::PathBuf::from)
                        .unwrap_or_default(),
                    self.intermediate_dir.clone(),
                    self.complete_dir.clone(),
                )
            });
        let failure_message = match &pipeline_outcome {
            crate::post_processing::model::PipelineOutcome::Failed { message, .. } => {
                Some(message.clone())
            }
            crate::post_processing::model::PipelineOutcome::Succeeded => None,
        };
        let context = crate::post_processing::runner::JobExecutionContext {
            job_id: job_id.0,
            name: state.spec.name.clone(),
            nzb_filename: format!("{}.nzb", state.spec.name),
            category: state.spec.category.clone(),
            group: None,
            source_url: None,
            working_directory: state.working_dir.clone(),
            final_directory: state.working_dir.clone(),
            pipeline_outcome: pipeline_outcome.clone(),
            par_status,
            unpack_status,
            compatibility: crate::post_processing::runner::CompatibilityFacts {
                total_bytes: state.spec.total_bytes,
                downloaded_bytes: state.downloaded_bytes,
                health_milli: health_milli(state.spec.total_bytes, state.failed_bytes),
                critical_health_milli: Self::critical_health_milli(
                    state.spec.total_bytes,
                    state.par2_bytes,
                ),
                password: state.spec.password.clone(),
                failure_message,
                data_dir: Some(data_dir),
                intermediate_dir: Some(intermediate_dir),
                complete_dir: Some(complete_dir),
                temp_dir: Some(std::env::temp_dir()),
                app_dir: std::env::current_exe()
                    .ok()
                    .and_then(|path| path.parent().map(std::path::PathBuf::from)),
                previous_script_status: Default::default(),
            },
        };
        self.transition_postprocessing_status(
            job_id,
            JobStatus::QueuedPostProcessing,
            Some("queued for extension post-processing"),
        );
        self.publish_snapshot();
        let (cancellation_tx, cancellation_rx) = tokio::sync::watch::channel(false);
        self.terminal_post_processing_cancellations
            .insert(job_id, cancellation_tx);
        let service = self.terminal_post_processing_service.clone();
        let done_tx = self.terminal_post_processing_done_tx.clone();
        tokio::spawn(async move {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let execution = service.execute_existing_with_started(
                &run_id,
                context,
                interpreters,
                Some(cancellation_rx),
                Some(started_tx),
            );
            tokio::pin!(execution);
            tokio::pin!(started_rx);
            let result = tokio::select! {
                result = &mut execution => result,
                started = &mut started_rx => {
                    if started.is_ok() {
                        let _ = done_tx
                            .send(TerminalPostProcessingEvent::Started(job_id))
                            .await;
                    }
                    execution.await
                }
            };
            let _ = done_tx
                .send(TerminalPostProcessingEvent::Done(
                    TerminalPostProcessingDone {
                        job_id,
                        primary_failure,
                        result,
                    },
                ))
                .await;
        });
    }

    /// Reconcile the durable post-processing state before the restored job can re-enter the
    /// built-in completion pipeline. Queued runs are safe to resume because no attempt has
    /// started; active attempts are marked interrupted during `Pipeline` construction and are
    /// deliberately never rerun here.
    pub(crate) fn recover_restored_terminal_post_processing(&mut self, job_id: JobId) -> bool {
        let is_terminal_post_processing = self.jobs.get(&job_id).is_some_and(|state| {
            matches!(
                state.status,
                JobStatus::QueuedPostProcessing | JobStatus::PostProcessing
            )
        });
        if !is_terminal_post_processing {
            return false;
        }
        self.remove_pending_completion_check(job_id);

        let run = match self.db.active_job_post_processing_run(job_id.0) {
            Ok(run) => run,
            Err(error) => {
                self.finalize_failed_job_after_terminal_post_processing(
                    job_id,
                    format!("failed to recover terminal post-processing state: {error}"),
                );
                return true;
            }
        };
        let Some(run) = run else {
            self.finalize_failed_job_after_terminal_post_processing(
                job_id,
                "terminal post-processing state was restored without a durable run".into(),
            );
            return true;
        };
        let primary_failure = match (&run.terminal_intent, &run.pipeline_outcome) {
            (
                crate::post_processing::persistence::TerminalIntent::Fail,
                crate::post_processing::model::PipelineOutcome::Failed { message, .. },
            ) => Some(message.clone()),
            (crate::post_processing::persistence::TerminalIntent::Fail, _) => {
                Some("built-in pipeline failed before post-processing".into())
            }
            _ => None,
        };

        if matches!(run.status, crate::post_processing::model::RunStatus::Queued) {
            if self.inflight_terminal_post_processing.insert(job_id) {
                info!(
                    job_id = job_id.0,
                    run_id = %run.run_id.as_str(),
                    "resuming durable queued post-processing run after restart"
                );
                self.launch_terminal_post_processing_run(
                    job_id,
                    run.run_id,
                    run.pipeline_outcome,
                    primary_failure,
                );
            }
            return true;
        }

        if matches!(
            run.status,
            crate::post_processing::model::RunStatus::Starting
                | crate::post_processing::model::RunStatus::Running
        ) {
            let _ = self.db.finish_post_processing_run(
                &run.run_id,
                crate::post_processing::model::RunStatus::Interrupted,
                crate::post_processing::model::PostProcessingSummary::Interrupted,
                chrono::Utc::now().timestamp_millis(),
            );
        }
        let effects = self
            .db
            .post_processing_attempts(&run.run_id)
            .unwrap_or_default()
            .into_iter()
            .map(|attempt| attempt.control_effects())
            .collect::<Vec<_>>();
        let repair_requested = effects.iter().any(|effect| effect.repair_requested);
        let mut restored_working_directory = self
            .jobs
            .get(&job_id)
            .map(|state| state.working_dir.clone());
        let mut restored_final_directory = None;
        for effect in &effects {
            if let Some(directory) = effect.directory.as_ref() {
                restored_working_directory = Some(directory.clone());
            }
            if let Some(final_directory) = effect.final_directory.as_ref() {
                restored_final_directory = Some(final_directory.clone());
            }
        }
        let output_directory = restored_final_directory.or(restored_working_directory);
        let summary = if matches!(
            run.status,
            crate::post_processing::model::RunStatus::Starting
                | crate::post_processing::model::RunStatus::Running
        ) {
            crate::post_processing::model::PostProcessingSummary::Interrupted
        } else {
            run.summary
        };
        info!(
            job_id = job_id.0,
            run_id = %run.run_id.as_str(),
            summary = ?summary,
            "finalizing restored terminal post-processing run without rerunning attempts"
        );
        self.handle_terminal_post_processing_done(TerminalPostProcessingDone {
            job_id,
            primary_failure,
            result: Ok(crate::post_processing::service::RunExecutionReport {
                run_id: Some(run.run_id),
                summary,
                effects,
                repair_requested,
                output_directory,
            }),
        });
        true
    }

    pub(crate) fn handle_terminal_post_processing_started(&mut self, job_id: JobId) {
        if !self.inflight_terminal_post_processing.contains(&job_id)
            || !self.jobs.contains_key(&job_id)
        {
            return;
        }
        self.transition_postprocessing_status(
            job_id,
            JobStatus::PostProcessing,
            Some("running extension post-processing"),
        );
        self.publish_snapshot();
    }

    pub(crate) fn handle_terminal_post_processing_done(
        &mut self,
        done: TerminalPostProcessingDone,
    ) {
        self.inflight_terminal_post_processing.remove(&done.job_id);
        self.terminal_post_processing_cancellations
            .remove(&done.job_id);
        if let Some(primary_failure) = done.primary_failure {
            match &done.result {
                Ok(report) => info!(
                    job_id = done.job_id.0,
                    summary = ?report.summary,
                    "failure post-processing finished; preserving primary pipeline failure"
                ),
                Err(error) => warn!(
                    job_id = done.job_id.0,
                    error = %error,
                    "failure post-processing could not complete; preserving primary pipeline failure"
                ),
            }
            if let Ok(report) = &done.result
                && let Some(path) = report.output_directory.as_ref()
                && let Some(state) = self.jobs.get_mut(&done.job_id)
            {
                state.working_dir = path.clone();
            }
            self.finalize_failed_job_after_terminal_post_processing(done.job_id, primary_failure);
            return;
        }
        match done.result {
            Ok(report)
                if matches!(
                    report.summary,
                    crate::post_processing::model::PostProcessingSummary::Succeeded
                        | crate::post_processing::model::PostProcessingSummary::Warning
                        | crate::post_processing::model::PostProcessingSummary::NotRun
                ) =>
            {
                if let Some(path) = report.output_directory.as_ref()
                    && let Some(state) = self.jobs.get_mut(&done.job_id)
                {
                    state.working_dir = path.clone();
                }
                if report.repair_requested {
                    let already_reentered =
                        self.post_processing_repair_reentered.contains(&done.job_id);
                    let recoverable_inputs_remain = self.par2_set(done.job_id).is_some()
                        && self
                            .jobs
                            .get(&done.job_id)
                            .is_some_and(|state| state.working_dir.is_dir());
                    if !already_reentered && recoverable_inputs_remain {
                        self.post_processing_repair_reentered.insert(done.job_id);
                        self.post_processing_repair_return_to_terminal
                            .insert(done.job_id);
                        self.par2_bypassed.remove(&done.job_id);
                        self.par2_verified.remove(&done.job_id);
                        info!(
                            job_id = done.job_id.0,
                            "extension requested PAR re-entry; returning to authoritative verification"
                        );
                        self.transition_postprocessing_status(
                            done.job_id,
                            JobStatus::Downloading,
                            Some("extension requested PAR verification"),
                        );
                        self.schedule_job_completion_check(done.job_id);
                        self.publish_snapshot();
                        return;
                    }
                    warn!(
                        job_id = done.job_id.0,
                        already_reentered,
                        recoverable_inputs_remain,
                        "ignoring extension PAR request because the one-time safety conditions were not met"
                    );
                }
                self.complete_job_after_terminal_post_processing(done.job_id);
            }
            Ok(report) => self.finalize_failed_job_after_terminal_post_processing(
                done.job_id,
                format!("extension post-processing ended with {:?}", report.summary),
            ),
            Err(error) => self.finalize_failed_job_after_terminal_post_processing(
                done.job_id,
                format!("extension post-processing failed: {error}"),
            ),
        }
    }

    fn complete_job_after_terminal_post_processing(&mut self, job_id: JobId) {
        self.transition_completed_runtime(job_id);
        if self.active_download_passes.remove(&job_id) {
            self.phase_end(job_id, JobPhase::Downloading);
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
            "job completed after terminal post-processing"
        );
        self.record_job_history(job_id);
        self.publish_snapshot();
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
