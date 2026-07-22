use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use tracing::{error, info, warn};

use crate::events::model::PipelineEvent;
use crate::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER;
use crate::jobs::assembly::{DetectedArchiveIdentity, JobAssembly};
use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};
use crate::jobs::model::{JobSpec, JobState, JobStatus};
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource};
use crate::jobs::working_dir::{compute_working_dir, working_dir_marker_path};
use crate::pipeline::{Pipeline, check_disk_space};
use crate::{DownloadQueue, DownloadWork, RestoreJobRequest};

#[derive(Debug, Default)]
struct RestoreSkipStats {
    complete_files: usize,
    complete_file_segments: usize,
    checkpoint_segments: usize,
    checkpoint_floor_bytes: u64,
    clamped_checkpoint_files: usize,
    missing_checkpoint_files: usize,
}

struct RestoreSkipPlan {
    skip: HashSet<SegmentId>,
    file_progress: HashMap<u32, u64>,
    stats: RestoreSkipStats,
}

impl Pipeline {
    async fn restore_has_open_download_finalization(&self, job_id: JobId) -> bool {
        let events = match self
            .db_blocking(move |db| db.get_job_events(job_id.0))
            .await
        {
            Ok(events) => events,
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load job events while restoring download finalization"
                );
                return false;
            }
        };

        let mut finalization_open = false;
        for event in events {
            match event.kind.as_str() {
                "JobCreated" | "JobCompleted" | "JobFailed" | "JobCancelled" => {
                    finalization_open = false;
                }
                "DownloadFinished"
                    if event.file_id.as_deref() == Some(JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER) =>
                {
                    finalization_open = true;
                }
                "DownloadPipelineDrained" => {
                    finalization_open = false;
                }
                _ => {}
            }
        }

        finalization_open
    }

    async fn restore_download_finalization_runtime(&mut self, job_id: JobId) -> bool {
        if !self.restore_has_open_download_finalization(job_id).await {
            self.jobs_finalizing_download.remove(&job_id);
            return false;
        }

        self.jobs_finalizing_download.insert(job_id);
        let _ = self.event_tx.send(PipelineEvent::DownloadFinished {
            job_id,
            finalization_pending: true,
        });
        true
    }

    fn normalize_restored_download_state(
        download_state: crate::jobs::model::DownloadState,
        download_queue: &DownloadQueue,
        recovery_queue: &DownloadQueue,
    ) -> crate::jobs::model::DownloadState {
        if !matches!(download_state, crate::jobs::model::DownloadState::Checking) {
            return download_state;
        }

        if download_queue.is_empty() && recovery_queue.is_empty() {
            crate::jobs::model::DownloadState::Complete
        } else {
            crate::jobs::model::DownloadState::Queued
        }
    }

    fn restored_download_state_from_status(
        status: &JobStatus,
        download_queue: &DownloadQueue,
        recovery_queue: &DownloadQueue,
    ) -> crate::jobs::model::DownloadState {
        let download_state = match status {
            JobStatus::Queued => crate::jobs::model::DownloadState::Queued,
            JobStatus::Checking => crate::jobs::model::DownloadState::Checking,
            JobStatus::Complete | JobStatus::Moving => crate::jobs::model::DownloadState::Complete,
            JobStatus::Failed { .. } => crate::jobs::model::DownloadState::Failed,
            _ => {
                if download_queue.is_empty() && recovery_queue.is_empty() {
                    crate::jobs::model::DownloadState::Complete
                } else {
                    crate::jobs::model::DownloadState::Downloading
                }
            }
        };
        Self::normalize_restored_download_state(download_state, download_queue, recovery_queue)
    }

    fn normalize_restored_status(
        status: JobStatus,
        download_queue: &DownloadQueue,
        recovery_queue: &DownloadQueue,
    ) -> JobStatus {
        if !matches!(status, JobStatus::Checking) {
            return status;
        }

        if download_queue.is_empty() && recovery_queue.is_empty() {
            JobStatus::Complete
        } else {
            JobStatus::Queued
        }
    }

    fn normalize_paused_resume_status(
        paused_resume_status: Option<JobStatus>,
    ) -> Option<JobStatus> {
        paused_resume_status.map(|status| match status {
            JobStatus::Queued => JobStatus::Queued,
            _ => JobStatus::Downloading,
        })
    }

    fn is_restartable_terminal_status(status: &JobStatus) -> bool {
        matches!(status, JobStatus::Complete | JobStatus::Failed { .. })
    }

    fn scrub_restored_par2_file_identities(
        file_identities: &mut HashMap<u32, ActiveFileIdentity>,
    ) -> (HashSet<String>, HashSet<u32>) {
        let mut stale_rar_sets = HashSet::new();
        let mut refreshed_rar_files = HashSet::new();

        for identity in file_identities.values_mut() {
            if identity.classification_source != FileIdentitySource::Par2 {
                continue;
            }

            let target_filename = identity
                .canonical_filename
                .as_deref()
                .unwrap_or(identity.current_filename.as_str());
            if identity.current_filename != target_filename {
                continue;
            }

            let Some(canonical_classification) =
                Self::canonical_archive_identity_from_filename(target_filename)
            else {
                continue;
            };
            if identity.classification.as_ref() == Some(&canonical_classification) {
                continue;
            }

            if let Some(set_name) = identity.classification.as_ref().and_then(|classification| {
                matches!(
                    classification.kind,
                    crate::jobs::assembly::DetectedArchiveKind::Rar
                )
                .then(|| classification.set_name.clone())
            }) && canonical_classification.set_name != set_name
            {
                stale_rar_sets.insert(set_name);
            }

            if matches!(
                canonical_classification.kind,
                crate::jobs::assembly::DetectedArchiveKind::Rar
            ) {
                refreshed_rar_files.insert(identity.file_index);
            }

            identity.classification = Some(canonical_classification);
        }

        (stale_rar_sets, refreshed_rar_files)
    }

    async fn load_history_row(
        &self,
        job_id: JobId,
    ) -> Result<Option<crate::JobHistoryRow>, crate::SchedulerError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.get_job_history_profiled(
                job_id.0,
                "db.get_job_history.jobs_service_load_history_row",
            )
        })
        .await
        .map_err(|error| {
            crate::SchedulerError::Internal(format!("failed to join history lookup task: {error}"))
        })?
        .map_err(crate::SchedulerError::State)
    }

    fn persisted_nzb_path_for_job(
        &self,
        job_id: JobId,
        history_row: Option<&crate::JobHistoryRow>,
    ) -> PathBuf {
        history_row
            .and_then(|row| row.nzb_path.as_deref())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("job-{}.nzb", job_id.0)))
    }

    async fn persist_file_identities(
        &self,
        job_id: JobId,
        identities: &HashMap<u32, ActiveFileIdentity>,
    ) {
        if identities.is_empty() {
            return;
        }
        let identities = identities.values().cloned().collect::<Vec<_>>();
        if let Err(error) = self
            .db_blocking(move |db| db.save_file_identities(job_id, &identities))
            .await
        {
            warn!(
                job_id = job_id.0,
                error = %error,
                "failed to persist file identities"
            );
        }
    }

    fn map_restart_nzb_error(
        job_id: JobId,
        error: crate::ingest::PersistedNzbError,
    ) -> crate::SchedulerError {
        match error {
            crate::ingest::PersistedNzbError::Io(inner) => {
                crate::SchedulerError::Io(std::io::Error::new(
                    inner.kind(),
                    format!("failed to read NZB for job {}: {inner}", job_id.0),
                ))
            }
            crate::ingest::PersistedNzbError::Parse(inner) => crate::SchedulerError::Internal(
                format!("failed to parse NZB for job {}: {inner}", job_id.0),
            ),
        }
    }

    fn parse_restart_nzb_bytes(
        &self,
        job_id: JobId,
        nzb_zstd: &[u8],
    ) -> Result<weaver_nzb::Nzb, crate::SchedulerError> {
        crate::ingest::parse_persisted_nzb_bytes(nzb_zstd)
            .map_err(|error| Self::map_restart_nzb_error(job_id, error))
    }

    fn missing_restart_nzb_error(
        job_id: JobId,
        nzb_path: &std::path::Path,
    ) -> crate::SchedulerError {
        crate::SchedulerError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "persisted NZB missing from database for job {} ({})",
                job_id.0,
                nzb_path.display()
            ),
        ))
    }

    fn load_restart_nzb(
        &self,
        job_id: JobId,
        nzb_path: &std::path::Path,
        nzb_zstd: Option<Vec<u8>>,
    ) -> Result<(weaver_nzb::Nzb, PathBuf, Vec<u8>), crate::SchedulerError> {
        let Some(nzb_zstd) = nzb_zstd else {
            return Err(Self::missing_restart_nzb_error(job_id, nzb_path));
        };

        let nzb = self.parse_restart_nzb_bytes(job_id, &nzb_zstd)?;
        Ok((nzb, nzb_path.to_path_buf(), nzb_zstd))
    }

    fn redownload_staging_dir(&self, job_id: JobId) -> PathBuf {
        self.complete_dir
            .join(".weaver-staging")
            .join(job_id.0.to_string())
    }

    async fn remove_redownload_artifacts(
        &self,
        job_id: JobId,
        working_dir: &std::path::Path,
        staging_dir: Option<&std::path::Path>,
    ) {
        // Redownload reuses these exact paths right away; a stale cached
        // write handle would swallow the new download's writes.
        crate::pipeline::close_cached_write_handles_under(working_dir).await;
        let paths = [
            Some(working_dir.to_path_buf()),
            staging_dir.map(|path| path.to_path_buf()),
            Some(self.redownload_staging_dir(job_id)),
        ];

        for path in paths.into_iter().flatten() {
            match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => {
                    info!(
                        job_id = job_id.0,
                        dir = %path.display(),
                        "removed redownload artifact directory"
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        dir = %path.display(),
                        error = %error,
                        "failed to remove redownload artifact directory"
                    );
                }
            }
        }
    }

    async fn delete_failed_history_entry(&mut self, job_id: JobId) {
        self.finished_jobs.retain(|job| job.job_id != job_id);
        let db = self.db.clone();
        match tokio::task::spawn_blocking(move || db.delete_job_history(job_id.0)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to delete failed job history entry during restart"
                );
            }
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to join failed job history delete task during restart"
                );
            }
        }
    }

    async fn reset_failed_job_runtime(&mut self, job_id: JobId) {
        self.remove_pending_completion_check(job_id);
        self.clear_terminal_segment_failures(job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_extraction_runtime(job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.replace_failed_extraction_members(job_id, HashSet::new());
        self.set_normalization_retried_state(job_id, false);
        if let Err(error) = self
            .db_blocking(move |db| db.clear_verified_suspect_volumes(job_id))
            .await
        {
            error!(
                job_id = job_id.0,
                error = %error,
                "failed to clear persisted verified suspect RAR volumes during failed-job restart"
            );
        }
    }

    fn declared_archive_identity(
        filename: &str,
        role: &weaver_model::files::FileRole,
    ) -> Option<DetectedArchiveIdentity> {
        let set_name = weaver_model::files::archive_base_name(filename, role)?;
        match role {
            weaver_model::files::FileRole::RarVolume { volume_number } => {
                Some(DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name,
                    volume_index: Some(*volume_number),
                })
            }
            weaver_model::files::FileRole::SevenZipArchive => Some(DetectedArchiveIdentity {
                kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSingle,
                set_name,
                volume_index: None,
            }),
            weaver_model::files::FileRole::SevenZipSplit { number } => {
                Some(DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSplit,
                    set_name,
                    volume_index: Some(*number),
                })
            }
            _ => None,
        }
    }

    fn default_file_identity(
        file_index: u32,
        filename: &str,
        role: &weaver_model::files::FileRole,
        detected: Option<&DetectedArchiveIdentity>,
    ) -> ActiveFileIdentity {
        ActiveFileIdentity {
            file_index,
            source_filename: filename.to_string(),
            current_filename: filename.to_string(),
            canonical_filename: None,
            classification: detected
                .cloned()
                .or_else(|| Self::declared_archive_identity(filename, role)),
            classification_source: if detected.is_some() {
                FileIdentitySource::Probe
            } else {
                FileIdentitySource::Declared
            },
        }
    }

    fn build_initial_file_identities(
        spec: &JobSpec,
        detected_archives: &HashMap<u32, DetectedArchiveIdentity>,
    ) -> HashMap<u32, ActiveFileIdentity> {
        spec.files
            .iter()
            .enumerate()
            .map(|(file_index, file_spec)| {
                let file_index = file_index as u32;
                (
                    file_index,
                    Self::default_file_identity(
                        file_index,
                        &file_spec.filename,
                        &file_spec.role,
                        detected_archives.get(&file_index),
                    ),
                )
            })
            .collect()
    }

    fn restore_current_filename<'a>(
        file_index: u32,
        file_spec: &'a crate::jobs::model::FileSpec,
        file_identities: &'a HashMap<u32, ActiveFileIdentity>,
    ) -> &'a str {
        file_identities
            .get(&file_index)
            .map(|identity| identity.current_filename.as_str())
            .unwrap_or(file_spec.filename.as_str())
    }

    async fn build_restore_skip_plan(
        job_id: JobId,
        spec: &JobSpec,
        complete_files: &HashSet<NzbFileId>,
        recovered_file_progress: &HashMap<u32, u64>,
        file_identities: &HashMap<u32, ActiveFileIdentity>,
        working_dir: &std::path::Path,
    ) -> RestoreSkipPlan {
        let mut skip = HashSet::new();
        let mut file_progress = HashMap::new();
        let mut stats = RestoreSkipStats::default();

        for (file_index, file_spec) in spec.files.iter().enumerate() {
            let file_index = file_index as u32;
            let file_id = NzbFileId { job_id, file_index };
            let file_total_bytes = file_spec
                .segments
                .iter()
                .map(|segment| segment.bytes as u64)
                .sum::<u64>();

            if complete_files.contains(&file_id) {
                stats.complete_files += 1;
                for segment in &file_spec.segments {
                    let segment_id = SegmentId {
                        file_id,
                        segment_number: segment.ordinal,
                    };
                    if skip.insert(segment_id) {
                        stats.complete_file_segments += 1;
                    }
                }
                file_progress.insert(file_index, file_total_bytes);
                continue;
            }

            let Some(raw_floor) = recovered_file_progress.get(&file_index).copied() else {
                continue;
            };
            if raw_floor == 0 {
                file_progress.insert(file_index, 0);
                continue;
            }

            let filename = Self::restore_current_filename(file_index, file_spec, file_identities);
            let metadata = match tokio::fs::metadata(working_dir.join(filename)).await {
                Ok(metadata) if metadata.is_file() => metadata,
                Ok(_) | Err(_) => {
                    stats.missing_checkpoint_files += 1;
                    file_progress.insert(file_index, 0);
                    continue;
                }
            };

            let clamped_floor = raw_floor.min(file_total_bytes).min(metadata.len());
            if clamped_floor < raw_floor.min(file_total_bytes) {
                stats.clamped_checkpoint_files += 1;
            }

            let mut segment_end = 0u64;
            let mut checkpoint_floor = 0u64;
            for segment in &file_spec.segments {
                segment_end = segment_end.saturating_add(segment.bytes as u64);
                if segment_end > clamped_floor {
                    break;
                }

                let segment_id = SegmentId {
                    file_id,
                    segment_number: segment.ordinal,
                };
                if skip.insert(segment_id) {
                    stats.checkpoint_segments += 1;
                }
                checkpoint_floor = segment_end;
            }

            stats.checkpoint_floor_bytes = stats
                .checkpoint_floor_bytes
                .saturating_add(checkpoint_floor);
            file_progress.insert(file_index, checkpoint_floor);
        }

        RestoreSkipPlan {
            skip,
            file_progress,
            stats,
        }
    }

    fn apply_detected_archive_identities(
        &mut self,
        job_id: JobId,
        detected_archives: &HashMap<u32, DetectedArchiveIdentity>,
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        state.detected_archives = detected_archives.clone();
    }

    pub(crate) async fn add_job(
        &mut self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: crate::jobs::AddJobOptions,
    ) -> Result<(), crate::SchedulerError> {
        let started = std::time::Instant::now();
        let _profile_scope = crate::runtime::perf_probe::scope("pipeline.add_job.total");
        let mut stage_start = std::time::Instant::now();
        if self.jobs.contains_key(&job_id) {
            return Err(crate::SchedulerError::JobExists(job_id));
        }

        if let Some(generation) = options.semantic_materialization_generation {
            let db = self.db.clone();
            let current = tokio::task::spawn_blocking(move || {
                db.semantic_materialization_is_current(job_id, generation)
            })
            .await
            .map_err(|error| {
                crate::SchedulerError::Internal(format!(
                    "semantic materialization check worker panicked: {error}"
                ))
            })??;
            if !current {
                return Err(crate::SchedulerError::SemanticSuperseded);
            }
        }
        if let Some(generation) = options.semantic_promotion_generation {
            let db = self.db.clone();
            let current = tokio::task::spawn_blocking(move || {
                db.semantic_promotion_materialization_is_current(job_id, generation)
            })
            .await
            .map_err(|error| {
                crate::SchedulerError::Internal(format!(
                    "semantic promotion check worker panicked: {error}"
                ))
            })??;
            if !current {
                return Err(crate::SchedulerError::SemanticSuperseded);
            }
        }

        let working_dir = compute_working_dir(&self.intermediate_dir, job_id, &spec.name);
        tokio::fs::create_dir_all(&working_dir)
            .await
            .map_err(crate::SchedulerError::Io)?;
        tokio::fs::write(working_dir_marker_path(&working_dir), [])
            .await
            .map_err(crate::SchedulerError::Io)?;
        crate::runtime::perf_probe::record(
            "pipeline.add_job.working_dir_ready",
            stage_start.elapsed(),
        );
        info!(
            job_id = job_id.0,
            working_dir = %working_dir.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "working_dir_ready",
            "pipeline add_job stage"
        );

        stage_start = std::time::Instant::now();
        let nzb_hash = crate::ingest::hash_persisted_nzb_bytes(&nzb_zstd);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &HashSet::new());
        let file_identities = Self::build_initial_file_identities(&spec, &HashMap::new());
        let initial_file_identities = file_identities.values().cloned().collect::<Vec<_>>();
        let (initial_status, initial_download_state, initial_post_state, initial_run_state) =
            if options.initially_paused {
                (
                    JobStatus::Paused,
                    crate::jobs::model::DownloadState::Queued,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Paused,
                )
            } else {
                (
                    JobStatus::Queued,
                    crate::jobs::model::DownloadState::Queued,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Active,
                )
            };
        crate::runtime::perf_probe::record(
            "pipeline.add_job.build_runtime_inputs",
            stage_start.elapsed(),
        );

        stage_start = std::time::Instant::now();
        let active_job = crate::ActiveJob {
            job_id,
            nzb_hash,
            nzb_path,
            nzb_zstd,
            output_dir: working_dir.clone(),
            created_at,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
            status: initial_status.persisted_status(),
            download_state: initial_download_state.as_str(),
            post_state: initial_post_state.as_str(),
            run_state: initial_run_state.as_str(),
            paused_resume_status: options
                .initially_paused
                .then_some(JobStatus::Queued.persisted_status()),
            paused_resume_download_state: options
                .initially_paused
                .then_some(crate::jobs::model::DownloadState::Queued.as_str()),
            paused_resume_post_state: options
                .initially_paused
                .then_some(crate::jobs::model::PostState::Idle.as_str()),
        };
        // Biggest single write on the add-job path: keep it off the
        // orchestrator loop, but never create an in-memory job without durable
        // active-job state. SCORE candidates additionally consume their
        // admission generation in this same transaction, closing the window
        // between a higher-score supersession and scheduler materialization.
        let db = self.db.clone();
        let semantic_materialization_generation = options.semantic_materialization_generation;
        let semantic_promotion_generation = options.semantic_promotion_generation;
        let persisted = tokio::task::spawn_blocking(move || {
            db.materialize_active_job_with_file_identities(
                &active_job,
                &initial_file_identities,
                semantic_materialization_generation,
                semantic_promotion_generation,
            )
        })
        .await
        .map_err(|error| {
            crate::SchedulerError::Internal(format!(
                "create active job persistence worker panicked: {error}"
            ))
        })?;
        match persisted {
            Err(error) => {
                if let Err(cleanup_error) = tokio::fs::remove_dir_all(&working_dir).await
                    && cleanup_error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(job_id = job_id.0, error = %cleanup_error, "failed to clean working directory after active job persistence failure");
                }
                return Err(error.into());
            }
            Ok(false) => {
                if let Err(cleanup_error) = tokio::fs::remove_dir_all(&working_dir).await
                    && cleanup_error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(job_id = job_id.0, error = %cleanup_error, "failed to clean superseded job working directory");
                }
                return Err(crate::SchedulerError::SemanticSuperseded);
            }
            Ok(true) => {}
        }
        crate::runtime::perf_probe::record(
            "pipeline.add_job.persist_active_job",
            stage_start.elapsed(),
        );
        info!(
            job_id = job_id.0,
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "active_job_persisted",
            "pipeline add_job stage"
        );

        check_disk_space(&self.intermediate_dir, spec.total_bytes);

        let queue_depth = download_queue.len() + recovery_queue.len();

        stage_start = std::time::Instant::now();
        let _ = self.event_tx.send(PipelineEvent::JobCreated {
            job_id,
            name: spec.name.clone(),
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
        });

        let par2_bytes = spec.par2_bytes();
        let mut state = JobState {
            job_id,
            job_hash: nzb_hash,
            spec,
            status: initial_status,
            download_state: initial_download_state,
            post_state: initial_post_state,
            run_state: initial_run_state,
            assembly,
            extraction_depth: 0,
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: options.initially_paused.then_some(JobStatus::Queued),
            paused_resume_download_state: options
                .initially_paused
                .then_some(crate::jobs::model::DownloadState::Queued),
            paused_resume_post_state: options
                .initially_paused
                .then_some(crate::jobs::model::PostState::Idle),
            failure_error: None,
            working_dir: working_dir.clone(),
            downloaded_bytes: 0,
            restored_download_floor_bytes: 0,
            failed_bytes: 0,
            par2_bytes,
            health_probing: false,
            health_probe_round: 0,
            last_health_probe_failed_bytes: 0,
            next_health_probe_failed_bytes: 1,
            detected_archives: HashMap::new(),
            file_identities,
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
            staging_dir: None,
        };
        state.refresh_runtime_lanes_from_status();
        self.jobs.insert(job_id, state);
        self.note_download_activity(job_id);
        self.job_order.push(job_id);

        crate::runtime::perf_probe::record(
            "pipeline.add_job.runtime_state_inserted",
            stage_start.elapsed(),
        );
        info!(
            job_id = job_id.0,
            queue_depth,
            working_dir = %working_dir.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "runtime_state_inserted",
            "job added"
        );
        Ok(())
    }

    pub(crate) fn build_job_assembly(
        job_id: JobId,
        spec: &JobSpec,
        skip: &HashSet<SegmentId>,
    ) -> (JobAssembly, DownloadQueue, DownloadQueue) {
        let mut assembly = JobAssembly::new(job_id);
        let mut download_queue = DownloadQueue::new();
        let mut recovery_queue = DownloadQueue::new();
        let mut has_par2_index = false;
        let mut recovery_files: Vec<(u32, u64)> = Vec::new();

        for (file_index, file_spec) in spec.files.iter().enumerate() {
            let file_id = NzbFileId {
                job_id,
                file_index: file_index as u32,
            };

            let segment_sizes: Vec<u32> = file_spec.segments.iter().map(|s| s.bytes).collect();

            let mut file_assembly = crate::jobs::assembly::FileAssembly::new(
                file_id,
                file_spec.filename.clone(),
                file_spec.role.clone(),
                segment_sizes,
            );

            if matches!(
                file_spec.role,
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
            ) {
                has_par2_index = true;
            }

            let priority = file_spec.role.download_priority();
            let is_recovery = file_spec.role.is_recovery();

            if is_recovery {
                let total: u64 = file_spec.segments.iter().map(|s| s.bytes as u64).sum();
                recovery_files.push((file_index as u32, total));
            }

            let target_queue = if is_recovery {
                &mut recovery_queue
            } else {
                &mut download_queue
            };

            for seg in &file_spec.segments {
                let segment_id = SegmentId {
                    file_id,
                    segment_number: seg.ordinal,
                };
                if skip.contains(&segment_id) {
                    let _ = file_assembly.commit_segment(seg.ordinal, seg.bytes);
                } else {
                    target_queue.push(DownloadWork {
                        segment_id,
                        message_id: MessageId::new(&seg.message_id),
                        groups: file_spec.groups.clone(),
                        priority,
                        byte_estimate: seg.bytes,
                        retry_count: 0,
                        is_recovery,
                        exclude_servers: vec![],
                    });
                }
            }

            assembly.add_file(file_assembly);
        }

        if !has_par2_index && !recovery_files.is_empty() {
            recovery_files.sort_by_key(|&(_, size)| size);
            let promoted_file_index = recovery_files[0].0;

            let items = recovery_queue.drain_all();
            for mut item in items {
                if item.segment_id.file_id.file_index == promoted_file_index {
                    item.priority = 0;
                    item.is_recovery = false;
                    download_queue.push(item);
                } else {
                    recovery_queue.push(item);
                }
            }
        }

        (assembly, download_queue, recovery_queue)
    }

    pub(crate) async fn reprocess_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        let in_jobs = self.jobs.contains_key(&job_id);

        if in_jobs {
            let state = self.jobs.get(&job_id).unwrap();
            if !Self::is_restartable_terminal_status(&state.status) {
                return Err(crate::SchedulerError::Conflict(format!(
                    "job {} is not complete or failed",
                    job_id.0
                )));
            }
        } else {
            let history_row = self.load_history_row(job_id).await?;
            let (nzb, nzb_path, nzb_hash, category, metadata, output_dir, downloaded_bytes) =
                if let Some(row) = history_row.as_ref() {
                    let status = crate::job_status_from_persisted_str(
                        &row.status,
                        row.error_message.as_deref(),
                    );
                    if !Self::is_restartable_terminal_status(&status) {
                        return Err(crate::SchedulerError::Conflict(format!(
                            "job {} is not complete or failed",
                            job_id.0
                        )));
                    }

                    let metadata = row
                        .metadata
                        .as_deref()
                        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
                        .unwrap_or_default();

                    let (preferred_nzb_path, nzb_zstd) = self
                        .db
                        .load_history_job_persisted_nzb(job_id.0)
                        .map_err(crate::SchedulerError::State)?
                        .unwrap_or_else(|| {
                            (self.persisted_nzb_path_for_job(job_id, Some(row)), None)
                        });
                    let (nzb, nzb_path, nzb_zstd) =
                        self.load_restart_nzb(job_id, &preferred_nzb_path, nzb_zstd)?;
                    let nzb_hash = crate::ingest::hash_persisted_nzb_bytes(&nzb_zstd);
                    (
                        nzb,
                        nzb_path,
                        nzb_hash,
                        row.category.clone(),
                        metadata,
                        row.output_dir.clone(),
                        row.downloaded_bytes,
                    )
                } else {
                    let history_entry = self.finished_jobs.iter().find(|job| job.job_id == job_id);
                    let Some(info) = history_entry else {
                        return Err(crate::SchedulerError::JobNotFound(job_id));
                    };
                    if !Self::is_restartable_terminal_status(&info.status) {
                        return Err(crate::SchedulerError::Conflict(format!(
                            "job {} is not complete or failed",
                            job_id.0
                        )));
                    }

                    let (nzb_path, nzb_zstd) = self
                        .db
                        .load_history_job_persisted_nzb(job_id.0)
                        .map_err(crate::SchedulerError::State)?
                        .unwrap_or_else(|| {
                            (
                                self.persisted_nzb_path_for_job(job_id, history_row.as_ref()),
                                None,
                            )
                        });
                    let (nzb, nzb_path, nzb_zstd) =
                        self.load_restart_nzb(job_id, &nzb_path, nzb_zstd)?;
                    let nzb_hash = crate::ingest::hash_persisted_nzb_bytes(&nzb_zstd);

                    (
                        nzb,
                        nzb_path,
                        nzb_hash,
                        info.category.clone(),
                        info.metadata.clone(),
                        info.output_dir.clone(),
                        info.downloaded_bytes,
                    )
                };

            let spec = crate::ingest::nzb_to_spec(&nzb, &nzb_path, category, metadata);

            let working_dir = output_dir
                .as_ref()
                .map(PathBuf::from)
                .unwrap_or_else(|| compute_working_dir(&self.intermediate_dir, job_id, &spec.name));
            if working_dir.starts_with(&self.intermediate_dir)
                && tokio::fs::try_exists(&working_dir).await.unwrap_or(false)
                && let Err(error) =
                    tokio::fs::write(working_dir_marker_path(&working_dir), []).await
            {
                warn!(
                    job_id = job_id.0,
                    dir = %working_dir.display(),
                    error = %error,
                    "failed to stamp reprocessed working directory as Weaver-owned"
                );
            }

            let all_segments = Self::all_segment_ids(job_id, &spec);
            let (assembly, download_queue, recovery_queue) =
                Self::build_job_assembly(job_id, &spec, &all_segments);
            let file_identities = Self::build_initial_file_identities(&spec, &HashMap::new());

            let par2_bytes = spec.par2_bytes();
            let mut state = JobState {
                job_id,
                job_hash: nzb_hash,
                spec,
                status: JobStatus::Downloading,
                download_state: crate::jobs::model::DownloadState::Downloading,
                post_state: crate::jobs::model::PostState::Idle,
                run_state: crate::jobs::model::RunState::Active,
                assembly,
                extraction_depth: 0,
                created_at: std::time::Instant::now(),
                created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
                queued_repair_at_epoch_ms: None,
                queued_extract_at_epoch_ms: None,
                paused_resume_status: None,
                paused_resume_download_state: None,
                paused_resume_post_state: None,
                failure_error: None,
                working_dir,
                downloaded_bytes,
                restored_download_floor_bytes: 0,
                failed_bytes: 0,
                par2_bytes,
                health_probing: false,
                health_probe_round: 0,
                last_health_probe_failed_bytes: 0,
                next_health_probe_failed_bytes: 1,
                detected_archives: HashMap::new(),
                file_identities,
                held_segments: Vec::new(),
                download_queue,
                recovery_queue,
                staging_dir: None,
            };
            state.refresh_runtime_lanes_from_status();
            self.jobs.insert(job_id, state);
            if let Some(file_identities) = self
                .jobs
                .get(&job_id)
                .map(|state| state.file_identities.clone())
            {
                self.persist_file_identities(job_id, &file_identities).await;
            }
            self.note_download_activity(job_id);
        }

        if in_jobs {
            let state = self.jobs.get(&job_id).unwrap();
            let all_segments = Self::all_segment_ids(job_id, &state.spec);
            let spec_clone = state.spec.clone();
            let (assembly, download_queue, recovery_queue) =
                Self::build_job_assembly(job_id, &spec_clone, &all_segments);
            let file_identities;
            {
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.assembly = assembly;
                state.failure_error = None;
                state.file_identities =
                    Self::build_initial_file_identities(&spec_clone, &HashMap::new());
                state.download_queue = download_queue;
                state.recovery_queue = recovery_queue;
                state.held_segments.clear();
                state.failed_bytes = 0;
                file_identities = state.file_identities.clone();
            }
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
            self.note_download_activity(job_id);
            self.persist_file_identities(job_id, &file_identities).await;
        }

        self.delete_failed_history_entry(job_id).await;
        self.reset_failed_job_runtime(job_id).await;

        if !self.job_order.contains(&job_id) {
            self.job_order.push(job_id);
        }

        let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });

        info!(job_id = job_id.0, "reprocessing terminal job");

        self.reload_metadata_from_disk(job_id).await;
        self.check_job_completion(job_id).await;

        Ok(())
    }

    pub(crate) async fn redownload_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        if let Some(state) = self.jobs.get(&job_id) {
            if !Self::is_restartable_terminal_status(&state.status) {
                return Err(crate::SchedulerError::Conflict(format!(
                    "job {} is not complete or failed",
                    job_id.0
                )));
            }

            let category = state.spec.category.clone();
            let metadata = state.spec.metadata.clone();
            let working_dir = state.working_dir.clone();
            let staging_dir = state.staging_dir.clone();
            let (nzb_path, nzb_zstd) = self
                .db
                .load_active_job_persisted_nzb(job_id)
                .map_err(crate::SchedulerError::State)?
                .ok_or(crate::SchedulerError::JobNotFound(job_id))?;
            let (nzb, nzb_path, nzb_zstd) = self.load_restart_nzb(job_id, &nzb_path, nzb_zstd)?;
            let spec = crate::ingest::nzb_to_spec(&nzb, &nzb_path, category, metadata);

            self.remove_redownload_artifacts(job_id, &working_dir, staging_dir.as_deref())
                .await;
            self.purge_terminal_job_runtime(job_id);
            self.db
                .delete_active_job(job_id)
                .map_err(crate::SchedulerError::State)?;
            self.add_job(
                job_id,
                spec,
                nzb_path,
                nzb_zstd,
                crate::jobs::AddJobOptions::default(),
            )
            .await?;
            self.reset_failed_job_runtime(job_id).await;
            self.reload_metadata_from_disk(job_id).await;
            info!(job_id = job_id.0, "re-downloading terminal job");
            return Ok(());
        }

        let history_row = self.load_history_row(job_id).await?;
        if let Some(row) = history_row.as_ref() {
            let status =
                crate::job_status_from_persisted_str(&row.status, row.error_message.as_deref());
            if !Self::is_restartable_terminal_status(&status) {
                return Err(crate::SchedulerError::Conflict(format!(
                    "job {} is not complete or failed",
                    job_id.0
                )));
            }

            let (nzb_path, nzb_zstd) = self
                .db
                .load_history_job_persisted_nzb(job_id.0)
                .map_err(crate::SchedulerError::State)?
                .unwrap_or_else(|| (self.persisted_nzb_path_for_job(job_id, Some(row)), None));
            let (nzb, nzb_path, nzb_zstd) = self.load_restart_nzb(job_id, &nzb_path, nzb_zstd)?;
            let metadata = row
                .metadata
                .as_deref()
                .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
                .unwrap_or_default();
            let spec = crate::ingest::nzb_to_spec(&nzb, &nzb_path, row.category.clone(), metadata);
            let working_dir = row
                .output_dir
                .as_deref()
                .map(PathBuf::from)
                .unwrap_or_else(|| compute_working_dir(&self.intermediate_dir, job_id, &spec.name));

            self.remove_redownload_artifacts(job_id, &working_dir, None)
                .await;
            self.add_job(
                job_id,
                spec,
                nzb_path,
                nzb_zstd,
                crate::jobs::AddJobOptions::default(),
            )
            .await?;
            self.delete_failed_history_entry(job_id).await;
            self.reset_failed_job_runtime(job_id).await;
            self.reload_metadata_from_disk(job_id).await;

            info!(job_id = job_id.0, "re-downloading terminal history job");

            return Ok(());
        }

        let history_entry = self.finished_jobs.iter().find(|job| job.job_id == job_id);
        let Some(info) = history_entry else {
            return Err(crate::SchedulerError::JobNotFound(job_id));
        };
        if !Self::is_restartable_terminal_status(&info.status) {
            return Err(crate::SchedulerError::Conflict(format!(
                "job {} is not complete or failed",
                job_id.0
            )));
        }

        let (nzb_path, nzb_zstd) = self
            .db
            .load_history_job_persisted_nzb(job_id.0)
            .map_err(crate::SchedulerError::State)?
            .unwrap_or_else(|| {
                (
                    self.persisted_nzb_path_for_job(job_id, history_row.as_ref()),
                    None,
                )
            });
        let (nzb, nzb_path, nzb_zstd) = self.load_restart_nzb(job_id, &nzb_path, nzb_zstd)?;
        let spec = crate::ingest::nzb_to_spec(
            &nzb,
            &nzb_path,
            info.category.clone(),
            info.metadata.clone(),
        );
        let working_dir = history_row
            .as_ref()
            .and_then(|row| row.output_dir.as_ref())
            .map(PathBuf::from)
            .unwrap_or_else(|| compute_working_dir(&self.intermediate_dir, job_id, &spec.name));

        self.remove_redownload_artifacts(job_id, &working_dir, None)
            .await;
        self.add_job(
            job_id,
            spec,
            nzb_path,
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await?;
        self.delete_failed_history_entry(job_id).await;
        self.reset_failed_job_runtime(job_id).await;
        self.reload_metadata_from_disk(job_id).await;

        info!(job_id = job_id.0, "re-downloading terminal history job");

        Ok(())
    }

    async fn restore_par2_state_from_disk(&mut self, job_id: JobId) {
        let files: Vec<(NzbFileId, weaver_model::files::FileRole)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .files()
                .map(|f| (f.file_id(), f.role().clone()))
                .collect()
        };

        self.par2_runtime.remove(&job_id);

        for (file_id, role) in &files {
            if matches!(
                role,
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
            ) {
                self.try_load_par2_metadata(job_id, *file_id).await;
            }
        }

        for (file_id, role) in &files {
            if matches!(
                role,
                weaver_model::files::FileRole::Par2 {
                    is_index: false,
                    ..
                }
            ) {
                if self.par2_set(job_id).is_some() {
                    self.try_merge_par2_recovery(job_id, *file_id).await;
                } else {
                    self.try_load_par2_metadata(job_id, *file_id).await;
                }
            }
        }

        self.reapply_promoted_recovery_queue(job_id);
    }

    async fn reload_metadata_from_disk(&mut self, job_id: JobId) {
        self.restore_par2_state_from_disk(job_id).await;
        match self.db.load_detected_archive_identities(job_id) {
            Ok(detected_archives) => {
                self.apply_detected_archive_identities(job_id, &detected_archives);
            }
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted detected archive identities"
                );
            }
        }
        self.restore_rar_state_for_job(job_id).await;

        let files: Vec<NzbFileId> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state.assembly.files().map(|file| file.file_id()).collect()
        };

        for file_id in files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, false)
                .await;
        }
    }

    pub(crate) fn all_segment_ids(job_id: JobId, spec: &JobSpec) -> HashSet<SegmentId> {
        let mut ids = HashSet::new();
        for (file_index, file_spec) in spec.files.iter().enumerate() {
            let file_id = NzbFileId {
                job_id,
                file_index: file_index as u32,
            };
            for seg in &file_spec.segments {
                ids.insert(SegmentId {
                    file_id,
                    segment_number: seg.ordinal,
                });
            }
        }
        ids
    }

    pub(crate) async fn restore_job(
        &mut self,
        request: RestoreJobRequest,
    ) -> Result<(), crate::SchedulerError> {
        let RestoreJobRequest {
            job_id,
            job_hash,
            spec,
            complete_files,
            file_progress,
            detected_archives,
            file_identities,
            extracted_members,
            status,
            download_state,
            post_state,
            run_state,
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status,
            paused_resume_download_state,
            paused_resume_post_state,
            working_dir,
        } = request;
        if self.jobs.contains_key(&job_id) {
            return Err(crate::SchedulerError::JobExists(job_id));
        }
        if working_dir.starts_with(&self.intermediate_dir)
            && tokio::fs::try_exists(&working_dir).await.unwrap_or(false)
            && let Err(error) = tokio::fs::write(working_dir_marker_path(&working_dir), []).await
        {
            tracing::warn!(
                job_id = job_id.0,
                dir = %working_dir.display(),
                error = %error,
                "failed to stamp restored working directory as Weaver-owned"
            );
        }

        let mut file_identities = if file_identities.is_empty() {
            Self::build_initial_file_identities(&spec, &detected_archives)
        } else {
            file_identities
        };
        let (stale_rar_sets, refreshed_rar_files) =
            Self::scrub_restored_par2_file_identities(&mut file_identities);
        let restore_skip_plan = Self::build_restore_skip_plan(
            job_id,
            &spec,
            &complete_files,
            &file_progress,
            &file_identities,
            &working_dir,
        )
        .await;
        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &restore_skip_plan.skip);
        let status = Self::normalize_restored_status(status, &download_queue, &recovery_queue);

        let skip_ref = &restore_skip_plan.skip;
        let mut downloaded_bytes = 0u64;
        let mut restored_download_floor_bytes = 0u64;
        for (fi, file_spec) in spec.files.iter().enumerate() {
            let file_id = NzbFileId {
                job_id,
                file_index: fi as u32,
            };
            let committed_bytes_for_file: u64 = file_spec
                .segments
                .iter()
                .filter_map(|seg| {
                    let sid = SegmentId {
                        file_id,
                        segment_number: seg.ordinal,
                    };
                    skip_ref.contains(&sid).then_some(seg.bytes as u64)
                })
                .sum();
            downloaded_bytes += committed_bytes_for_file;
            let file_total_bytes: u64 = file_spec.segments.iter().map(|seg| seg.bytes as u64).sum();
            let file_progress_floor = restore_skip_plan
                .file_progress
                .get(&(fi as u32))
                .copied()
                .unwrap_or(0)
                .min(file_total_bytes);
            restored_download_floor_bytes += committed_bytes_for_file.max(file_progress_floor);
            self.persisted_file_progress
                .insert(file_id, file_progress_floor);
        }

        let queue_depth = download_queue.len() + recovery_queue.len();
        let paused_resume_status = matches!(status, JobStatus::Paused)
            .then(|| {
                paused_resume_status.clone().or_else(|| {
                    paused_resume_download_state
                        .zip(paused_resume_post_state)
                        .map(|(download_state, post_state)| {
                            crate::jobs::model::derive_legacy_job_status(
                                download_state,
                                post_state,
                                crate::jobs::model::RunState::Active,
                                None,
                            )
                        })
                })
            })
            .flatten();
        let paused_resume_status = Self::normalize_paused_resume_status(paused_resume_status);
        let restored_staging_dir = {
            let staging_dir = self.deterministic_extraction_staging_dir(job_id);
            tokio::fs::try_exists(&staging_dir)
                .await
                .unwrap_or(false)
                .then_some(staging_dir)
        };

        let _ = self.event_tx.send(PipelineEvent::JobCreated {
            job_id,
            name: spec.name.clone(),
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
        });

        let par2_bytes = spec.par2_bytes();
        let mut state = JobState {
            job_id,
            job_hash,
            spec,
            status: status.clone(),
            download_state: Self::normalize_restored_download_state(
                download_state.unwrap_or_else(|| {
                    Self::restored_download_state_from_status(
                        &status,
                        &download_queue,
                        &recovery_queue,
                    )
                }),
                &download_queue,
                &recovery_queue,
            ),
            post_state: post_state.unwrap_or(match &status {
                JobStatus::Queued => crate::jobs::model::PostState::Idle,
                JobStatus::Verifying => crate::jobs::model::PostState::Verifying,
                JobStatus::QueuedRepair => crate::jobs::model::PostState::QueuedRepair,
                JobStatus::Repairing => crate::jobs::model::PostState::Repairing,
                JobStatus::QueuedExtract => crate::jobs::model::PostState::QueuedExtract,
                JobStatus::Extracting => crate::jobs::model::PostState::Extracting,
                JobStatus::Moving => crate::jobs::model::PostState::Finalizing,
                JobStatus::Complete => crate::jobs::model::PostState::Completed,
                JobStatus::Failed { .. } => crate::jobs::model::PostState::Failed,
                JobStatus::Paused => crate::jobs::model::PostState::Idle,
                JobStatus::Downloading | JobStatus::Checking => crate::jobs::model::PostState::Idle,
            }),
            run_state: run_state.unwrap_or({
                if matches!(status, JobStatus::Paused) {
                    crate::jobs::model::RunState::Paused
                } else {
                    crate::jobs::model::RunState::Active
                }
            }),
            assembly,
            extraction_depth: 0,
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status: paused_resume_status.clone(),
            paused_resume_download_state: paused_resume_download_state.or_else(|| {
                paused_resume_status.as_ref().map(|status| {
                    Self::restored_download_state_from_status(
                        status,
                        &download_queue,
                        &recovery_queue,
                    )
                })
            }),
            paused_resume_post_state: paused_resume_post_state.or_else(|| {
                paused_resume_status.as_ref().map(|status| match status {
                    JobStatus::Verifying => crate::jobs::model::PostState::Verifying,
                    JobStatus::QueuedRepair => crate::jobs::model::PostState::QueuedRepair,
                    JobStatus::Repairing => crate::jobs::model::PostState::Repairing,
                    JobStatus::QueuedExtract => crate::jobs::model::PostState::QueuedExtract,
                    JobStatus::Extracting => crate::jobs::model::PostState::Extracting,
                    JobStatus::Moving => crate::jobs::model::PostState::Finalizing,
                    JobStatus::Complete => crate::jobs::model::PostState::Completed,
                    JobStatus::Failed { .. } => crate::jobs::model::PostState::Failed,
                    _ => crate::jobs::model::PostState::Idle,
                })
            }),
            failure_error: match status {
                JobStatus::Failed { ref error } => Some(error.clone()),
                _ => None,
            },
            working_dir,
            downloaded_bytes,
            restored_download_floor_bytes,
            failed_bytes: 0,
            par2_bytes,
            health_probing: false,
            health_probe_round: 0,
            last_health_probe_failed_bytes: 0,
            next_health_probe_failed_bytes: 1,
            detected_archives: HashMap::new(),
            file_identities,
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
            staging_dir: restored_staging_dir,
        };
        state.refresh_runtime_lanes_from_status();
        self.jobs.insert(job_id, state);
        self.restore_download_finalization_runtime(job_id).await;
        self.note_download_activity(job_id);
        self.job_order.push(job_id);
        if let Some(file_identities) = self
            .jobs
            .get(&job_id)
            .map(|state| state.file_identities.clone())
        {
            self.persist_file_identities(job_id, &file_identities).await;
        }
        for set_name in &stale_rar_sets {
            self.clear_archive_set_for_source_retry(job_id, set_name);
        }
        self.apply_detected_archive_identities(job_id, &detected_archives);
        if !extracted_members.is_empty() {
            self.extracted_members.insert(job_id, extracted_members);
        }
        match self.db.load_active_job_normalization_retried(job_id) {
            Ok(true) => {
                self.normalization_retried.insert(job_id);
            }
            Ok(false) => {}
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted normalization retry state"
                );
            }
        }
        self.reload_metadata_from_disk(job_id).await;
        let mut archive_refresh_file_indices = refreshed_rar_files;
        archive_refresh_file_indices.extend(
            complete_files
                .iter()
                .filter(|file_id| file_id.job_id == job_id)
                .map(|file_id| file_id.file_index),
        );
        for file_index in archive_refresh_file_indices {
            self.refresh_archive_state_for_completed_file(
                job_id,
                NzbFileId { job_id, file_index },
                false,
            )
            .await;
        }
        self.reconcile_job_progress(job_id).await;

        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| matches!(state.status, JobStatus::Repairing))
        {
            self.metrics.repair_active.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| matches!(state.status, JobStatus::Extracting))
        {
            self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
        }

        let (queued_repair_at_epoch_ms, queued_extract_at_epoch_ms) = self
            .jobs
            .get(&job_id)
            .map(|state| {
                (
                    state.queued_repair_at_epoch_ms,
                    state.queued_extract_at_epoch_ms,
                )
            })
            .unwrap_or((None, None));

        info!(
            job_id = job_id.0,
            downloaded_bytes,
            restored_download_floor_bytes,
            status = ?status,
            queued_repair_at_epoch_ms = queued_repair_at_epoch_ms.unwrap_or(0.0),
            queued_extract_at_epoch_ms = queued_extract_at_epoch_ms.unwrap_or(0.0),
            queue_depth,
            restore_skip_segments = restore_skip_plan.skip.len(),
            complete_restore_files = restore_skip_plan.stats.complete_files,
            complete_restore_segments = restore_skip_plan.stats.complete_file_segments,
            checkpoint_restore_segments = restore_skip_plan.stats.checkpoint_segments,
            checkpoint_floor_bytes = restore_skip_plan.stats.checkpoint_floor_bytes,
            clamped_checkpoint_files = restore_skip_plan.stats.clamped_checkpoint_files,
            missing_checkpoint_files = restore_skip_plan.stats.missing_checkpoint_files,
            "job restored from journal"
        );
        if self.jobs.get(&job_id).is_some_and(|state| {
            !matches!(
                state.status,
                JobStatus::Paused
                    | JobStatus::Moving
                    | JobStatus::Complete
                    | JobStatus::Failed { .. }
            )
        }) {
            self.schedule_job_completion_check(job_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
    use crate::jobs::{FileSpec, JobSpec, SegmentSpec};
    use weaver_model::files::FileRole;

    fn sparse_segment_job_spec() -> JobSpec {
        JobSpec {
            name: "Sparse Segments".to_string(),
            password: None,
            total_bytes: 60,
            category: None,
            metadata: vec![],
            files: vec![FileSpec {
                filename: "episode.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    SegmentSpec {
                        ordinal: 0,
                        article_number: 1,
                        bytes: 10,
                        message_id: "one@example.com".to_string(),
                    },
                    SegmentSpec {
                        ordinal: 1,
                        article_number: 4,
                        bytes: 20,
                        message_id: "four@example.com".to_string(),
                    },
                    SegmentSpec {
                        ordinal: 2,
                        article_number: 6,
                        bytes: 30,
                        message_id: "six@example.com".to_string(),
                    },
                ],
            }],
        }
    }

    fn sparse_segment_id(job_id: JobId, segment_number: u32) -> SegmentId {
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        }
    }

    #[tokio::test]
    async fn restore_skip_plan_uses_complete_files_without_segment_rows() {
        let job_id = JobId(43);
        let spec = sparse_segment_job_spec();
        let temp_dir = tempfile::tempdir().unwrap();
        let complete_files = HashSet::from([NzbFileId {
            job_id,
            file_index: 0,
        }]);

        let plan = Pipeline::build_restore_skip_plan(
            job_id,
            &spec,
            &complete_files,
            &HashMap::new(),
            &HashMap::new(),
            temp_dir.path(),
        )
        .await;

        assert_eq!(plan.skip.len(), 3);
        assert!(plan.skip.contains(&sparse_segment_id(job_id, 0)));
        assert!(plan.skip.contains(&sparse_segment_id(job_id, 1)));
        assert!(plan.skip.contains(&sparse_segment_id(job_id, 2)));
        assert_eq!(plan.file_progress.get(&0), Some(&60));
        assert_eq!(plan.stats.complete_files, 1);
        assert_eq!(plan.stats.complete_file_segments, 3);
    }

    #[tokio::test]
    async fn restore_skip_plan_uses_checkpointed_full_segments() {
        let job_id = JobId(44);
        let spec = sparse_segment_job_spec();
        let temp_dir = tempfile::tempdir().unwrap();
        tokio::fs::write(temp_dir.path().join("episode.bin"), vec![0u8; 60])
            .await
            .unwrap();

        let plan = Pipeline::build_restore_skip_plan(
            job_id,
            &spec,
            &HashSet::new(),
            &HashMap::from([(0u32, 30u64)]),
            &HashMap::new(),
            temp_dir.path(),
        )
        .await;

        assert_eq!(plan.skip.len(), 2);
        assert!(plan.skip.contains(&sparse_segment_id(job_id, 0)));
        assert!(plan.skip.contains(&sparse_segment_id(job_id, 1)));
        assert!(!plan.skip.contains(&sparse_segment_id(job_id, 2)));
        assert_eq!(plan.file_progress.get(&0), Some(&30));
        assert_eq!(plan.stats.checkpoint_segments, 2);
    }

    #[tokio::test]
    async fn restore_skip_plan_does_not_skip_partial_segment_floor() {
        let job_id = JobId(45);
        let spec = sparse_segment_job_spec();
        let temp_dir = tempfile::tempdir().unwrap();
        tokio::fs::write(temp_dir.path().join("episode.bin"), vec![0u8; 60])
            .await
            .unwrap();

        let plan = Pipeline::build_restore_skip_plan(
            job_id,
            &spec,
            &HashSet::new(),
            &HashMap::from([(0u32, 25u64)]),
            &HashMap::new(),
            temp_dir.path(),
        )
        .await;

        assert_eq!(plan.skip, HashSet::from([sparse_segment_id(job_id, 0)]));
        assert_eq!(plan.file_progress.get(&0), Some(&10));
        assert_eq!(plan.stats.checkpoint_segments, 1);
    }

    #[tokio::test]
    async fn restore_skip_plan_clamps_progress_to_partial_file_size() {
        let job_id = JobId(46);
        let spec = sparse_segment_job_spec();
        let temp_dir = tempfile::tempdir().unwrap();
        tokio::fs::write(temp_dir.path().join("episode.bin"), vec![0u8; 15])
            .await
            .unwrap();

        let plan = Pipeline::build_restore_skip_plan(
            job_id,
            &spec,
            &HashSet::new(),
            &HashMap::from([(0u32, 50u64)]),
            &HashMap::new(),
            temp_dir.path(),
        )
        .await;

        assert_eq!(plan.skip, HashSet::from([sparse_segment_id(job_id, 0)]));
        assert_eq!(plan.file_progress.get(&0), Some(&10));
        assert_eq!(plan.stats.clamped_checkpoint_files, 1);
    }

    #[tokio::test]
    async fn restore_skip_plan_discards_progress_when_partial_file_is_missing() {
        let job_id = JobId(47);
        let spec = sparse_segment_job_spec();
        let temp_dir = tempfile::tempdir().unwrap();

        let plan = Pipeline::build_restore_skip_plan(
            job_id,
            &spec,
            &HashSet::new(),
            &HashMap::from([(0u32, 50u64)]),
            &HashMap::new(),
            temp_dir.path(),
        )
        .await;

        assert!(plan.skip.is_empty());
        assert_eq!(plan.file_progress.get(&0), Some(&0));
        assert_eq!(plan.stats.missing_checkpoint_files, 1);
    }

    #[test]
    fn sparse_article_numbers_queue_dense_ordinals() {
        let job_id = JobId(41);
        let spec = sparse_segment_job_spec();
        let mut all_ids = Pipeline::all_segment_ids(job_id, &spec)
            .into_iter()
            .map(|segment_id| segment_id.segment_number)
            .collect::<Vec<_>>();
        all_ids.sort_unstable();
        assert_eq!(all_ids, vec![0, 1, 2]);

        let (assembly, mut download_queue, recovery_queue) =
            Pipeline::build_job_assembly(job_id, &spec, &HashSet::new());

        let mut queued = Vec::new();
        while let Some(work) = download_queue.pop() {
            queued.push(work.segment_id.segment_number);
        }
        queued.sort_unstable();

        assert_eq!(queued, vec![0, 1, 2]);
        assert!(recovery_queue.is_empty());
        assert_eq!(
            assembly
                .file(NzbFileId {
                    job_id,
                    file_index: 0,
                })
                .unwrap()
                .total_segments(),
            3
        );
    }

    #[test]
    fn sparse_article_numbers_do_not_overflow_committed_assembly() {
        let job_id = JobId(42);
        let spec = sparse_segment_job_spec();
        let committed = Pipeline::all_segment_ids(job_id, &spec);
        let (assembly, mut download_queue, recovery_queue) =
            Pipeline::build_job_assembly(job_id, &spec, &committed);

        assert!(download_queue.pop().is_none());
        assert!(recovery_queue.is_empty());
        assert!(
            assembly
                .file(NzbFileId {
                    job_id,
                    file_index: 0,
                })
                .unwrap()
                .is_complete()
        );
    }
}
