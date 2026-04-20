use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use tracing::{error, info, warn};

use crate::events::model::PipelineEvent;
use crate::jobs::assembly::{DetectedArchiveIdentity, JobAssembly};
use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};
use crate::jobs::model::{JobSpec, JobState, JobStatus};
use crate::jobs::record::{ActiveFileIdentity, FileIdentitySource};
use crate::jobs::working_dir::{compute_working_dir, working_dir_marker_path};
use crate::pipeline::{Pipeline, check_disk_space};
use crate::{DownloadQueue, DownloadWork, RestoreJobRequest};

impl Pipeline {
    async fn load_history_row(
        &self,
        job_id: JobId,
    ) -> Result<Option<crate::JobHistoryRow>, crate::SchedulerError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.get_job_history(job_id.0))
            .await
            .map_err(|error| {
                crate::SchedulerError::Internal(format!(
                    "failed to join history lookup task: {error}"
                ))
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
            .unwrap_or_else(|| self.nzb_dir.join(format!("{}.nzb", job_id.0)))
    }

    fn parse_restart_nzb(
        &self,
        job_id: JobId,
        nzb_path: &std::path::Path,
    ) -> Result<weaver_nzb::Nzb, crate::SchedulerError> {
        crate::ingest::parse_persisted_nzb(nzb_path).map_err(|error| match error {
            crate::ingest::PersistedNzbError::Io(inner) => {
                crate::SchedulerError::Io(std::io::Error::new(
                    inner.kind(),
                    format!("failed to read NZB for job {}: {inner}", job_id.0),
                ))
            }
            crate::ingest::PersistedNzbError::Parse(inner) => crate::SchedulerError::Internal(
                format!("failed to parse NZB for job {}: {inner}", job_id.0),
            ),
        })
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

    fn reset_failed_job_runtime(&mut self, job_id: JobId) {
        self.remove_pending_completion_check(job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_extraction_runtime(job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.replace_failed_extraction_members(job_id, HashSet::new());
        self.set_normalization_retried_state(job_id, false);
        if let Err(error) = self.db.clear_verified_suspect_volumes(job_id) {
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

    fn persist_file_identities(
        &self,
        job_id: JobId,
        file_identities: &HashMap<u32, ActiveFileIdentity>,
    ) {
        for identity in file_identities.values() {
            if let Err(error) = self.db.save_file_identity(job_id, identity) {
                error!(
                    job_id = job_id.0,
                    file_index = identity.file_index,
                    error = %error,
                    "db write failed for save_file_identity"
                );
            }
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
    ) -> Result<(), crate::SchedulerError> {
        let started = std::time::Instant::now();
        if self.jobs.contains_key(&job_id) {
            return Err(crate::SchedulerError::JobExists(job_id));
        }

        let working_dir = compute_working_dir(&self.intermediate_dir, job_id, &spec.name);
        tokio::fs::create_dir_all(&working_dir)
            .await
            .map_err(crate::SchedulerError::Io)?;
        tokio::fs::write(working_dir_marker_path(&working_dir), [])
            .await
            .map_err(crate::SchedulerError::Io)?;
        info!(
            job_id = job_id.0,
            working_dir = %working_dir.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "working_dir_ready",
            "pipeline add_job stage"
        );

        let nzb_hash = crate::ingest::hash_persisted_nzb_or_empty(&nzb_path);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Err(error) = self.db.create_active_job(&crate::ActiveJob {
            job_id,
            nzb_hash,
            nzb_path,
            output_dir: working_dir.clone(),
            created_at,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
        }) {
            error!(error = %error, "db write failed for create_active_job");
        }
        info!(
            job_id = job_id.0,
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "active_job_persisted",
            "pipeline add_job stage"
        );

        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &HashSet::new());
        let file_identities = Self::build_initial_file_identities(&spec, &HashMap::new());
        self.persist_file_identities(job_id, &file_identities);

        check_disk_space(&self.intermediate_dir, spec.total_bytes);

        let queue_depth = download_queue.len() + recovery_queue.len();

        let _ = self.event_tx.send(PipelineEvent::JobCreated {
            job_id,
            name: spec.name.clone(),
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
        });

        let par2_bytes = spec.par2_bytes();
        let state = JobState {
            job_id,
            spec,
            status: JobStatus::Queued,
            assembly,
            extraction_depth: 0,
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            working_dir: working_dir.clone(),
            downloaded_bytes: 0,
            restored_download_floor_bytes: 0,
            failed_bytes: 0,
            par2_bytes,
            health_probing: false,
            last_health_probe_failed_bytes: 0,
            detected_archives: HashMap::new(),
            file_identities,
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
            staging_dir: None,
        };
        self.jobs.insert(job_id, state);
        self.note_download_activity(job_id);
        self.job_order.push(job_id);

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
                    segment_number: seg.number,
                };
                if skip.contains(&segment_id) {
                    let _ = file_assembly.commit_segment(seg.number, seg.bytes);
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
            if !matches!(state.status, JobStatus::Failed { .. }) {
                return Err(crate::SchedulerError::Conflict(format!(
                    "job {} is not failed",
                    job_id.0
                )));
            }
        } else {
            let history_entry = self.finished_jobs.iter().find(|job| job.job_id == job_id);
            let Some(info) = history_entry else {
                return Err(crate::SchedulerError::JobNotFound(job_id));
            };
            if !matches!(info.status, JobStatus::Failed { .. }) {
                return Err(crate::SchedulerError::Conflict(format!(
                    "job {} is not failed",
                    job_id.0
                )));
            }

            let history_row = self.load_history_row(job_id).await?;
            let nzb_path = self.persisted_nzb_path_for_job(job_id, history_row.as_ref());
            let nzb = self.parse_restart_nzb(job_id, &nzb_path)?;

            let spec = crate::ingest::nzb_to_spec(
                &nzb,
                &nzb_path,
                info.category.clone(),
                info.metadata.clone(),
            );

            let working_dir = info
                .output_dir
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
            let state = JobState {
                job_id,
                spec,
                status: JobStatus::Downloading,
                assembly,
                extraction_depth: 0,
                created_at: std::time::Instant::now(),
                created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
                queued_repair_at_epoch_ms: None,
                queued_extract_at_epoch_ms: None,
                paused_resume_status: None,
                working_dir,
                downloaded_bytes: info.downloaded_bytes,
                restored_download_floor_bytes: 0,
                failed_bytes: 0,
                par2_bytes,
                health_probing: false,
                last_health_probe_failed_bytes: 0,
                detected_archives: HashMap::new(),
                file_identities,
                held_segments: Vec::new(),
                download_queue,
                recovery_queue,
                staging_dir: None,
            };
            self.jobs.insert(job_id, state);
            if let Some(state) = self.jobs.get(&job_id) {
                self.persist_file_identities(job_id, &state.file_identities);
            }
            self.note_download_activity(job_id);
        }

        if in_jobs {
            let state = self.jobs.get(&job_id).unwrap();
            let all_segments = Self::all_segment_ids(job_id, &state.spec);
            let spec_clone = state.spec.clone();
            let (assembly, download_queue, recovery_queue) =
                Self::build_job_assembly(job_id, &spec_clone, &all_segments);
            let state = self.jobs.get_mut(&job_id).unwrap();
            state.assembly = assembly;
            state.status = JobStatus::Downloading;
            state.file_identities =
                Self::build_initial_file_identities(&spec_clone, &HashMap::new());
            state.download_queue = download_queue;
            state.recovery_queue = recovery_queue;
            state.held_segments.clear();
            state.failed_bytes = 0;
            let file_identities = state.file_identities.clone();
            let _ = state;
            self.note_download_activity(job_id);
            self.persist_file_identities(job_id, &file_identities);
        }

        self.delete_failed_history_entry(job_id).await;
        self.reset_failed_job_runtime(job_id);

        if !self.job_order.contains(&job_id) {
            self.job_order.push(job_id);
        }

        let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });

        info!(job_id = job_id.0, "reprocessing failed job");

        self.reload_metadata_from_disk(job_id).await;
        self.check_job_completion(job_id).await;

        Ok(())
    }

    pub(crate) async fn redownload_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        if let Some(state) = self.jobs.get(&job_id) {
            if !matches!(state.status, JobStatus::Failed { .. }) {
                return Err(crate::SchedulerError::Conflict(format!(
                    "job {} is not failed",
                    job_id.0
                )));
            }

            let category = state.spec.category.clone();
            let metadata = state.spec.metadata.clone();
            let working_dir = state.working_dir.clone();
            let staging_dir = state.staging_dir.clone();
            let nzb_path = self.nzb_dir.join(format!("{}.nzb", job_id.0));
            let nzb = self.parse_restart_nzb(job_id, &nzb_path)?;
            let spec = crate::ingest::nzb_to_spec(&nzb, &nzb_path, category, metadata);

            self.remove_redownload_artifacts(job_id, &working_dir, staging_dir.as_deref())
                .await;
            self.purge_terminal_job_runtime(job_id);
            self.db
                .delete_active_job(job_id)
                .map_err(crate::SchedulerError::State)?;
            self.add_job(job_id, spec, nzb_path).await?;
            self.reset_failed_job_runtime(job_id);
            self.reload_metadata_from_disk(job_id).await;
            info!(job_id = job_id.0, "re-downloading failed job");
            return Ok(());
        }

        let history_entry = self.finished_jobs.iter().find(|job| job.job_id == job_id);
        let Some(info) = history_entry else {
            return Err(crate::SchedulerError::JobNotFound(job_id));
        };
        if !matches!(info.status, JobStatus::Failed { .. }) {
            return Err(crate::SchedulerError::Conflict(format!(
                "job {} is not failed",
                job_id.0
            )));
        }

        let history_row = self.load_history_row(job_id).await?;
        let nzb_path = self.persisted_nzb_path_for_job(job_id, history_row.as_ref());
        let nzb = self.parse_restart_nzb(job_id, &nzb_path)?;
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
        self.add_job(job_id, spec, nzb_path).await?;
        self.delete_failed_history_entry(job_id).await;
        self.reset_failed_job_runtime(job_id);
        self.reload_metadata_from_disk(job_id).await;

        info!(job_id = job_id.0, "re-downloading failed history job");

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

        match self.db.load_par2_files(job_id) {
            Ok(files) if !files.is_empty() => {
                let runtime = self.ensure_par2_runtime(job_id);
                for (file_index, file) in files {
                    runtime.files.insert(
                        file_index,
                        crate::pipeline::Par2FileRuntime {
                            filename: file.filename,
                            recovery_blocks: file.recovery_block_count,
                            promoted: file.promoted,
                        },
                    );
                }
            }
            Ok(_) => {}
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted PAR2 file state"
                );
            }
        }

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
            state.assembly.files().map(|f| f.file_id()).collect()
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
                    segment_number: seg.number,
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
            spec,
            committed_segments,
            file_progress,
            detected_archives,
            file_identities,
            extracted_members,
            status,
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status,
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

        let committed_count = committed_segments.len();
        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &committed_segments);

        let committed_ref = &committed_segments;
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
                        segment_number: seg.number,
                    };
                    committed_ref.contains(&sid).then_some(seg.bytes as u64)
                })
                .sum();
            downloaded_bytes += committed_bytes_for_file;
            let file_total_bytes: u64 = file_spec.segments.iter().map(|seg| seg.bytes as u64).sum();
            let file_progress_floor = file_progress
                .get(&(fi as u32))
                .copied()
                .unwrap_or(0)
                .min(file_total_bytes);
            restored_download_floor_bytes += committed_bytes_for_file.max(file_progress_floor);
            self.persisted_file_progress
                .insert(file_id, file_progress_floor);
        }

        let queue_depth = download_queue.len() + recovery_queue.len();
        let file_identities = if file_identities.is_empty() {
            Self::build_initial_file_identities(&spec, &detected_archives)
        } else {
            file_identities
        };

        let _ = self.event_tx.send(PipelineEvent::JobCreated {
            job_id,
            name: spec.name.clone(),
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
        });

        let par2_bytes = spec.par2_bytes();
        let state = JobState {
            job_id,
            spec,
            status: status.clone(),
            assembly,
            extraction_depth: 0,
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status,
            working_dir,
            downloaded_bytes,
            restored_download_floor_bytes,
            failed_bytes: 0,
            par2_bytes,
            health_probing: false,
            last_health_probe_failed_bytes: 0,
            detected_archives: HashMap::new(),
            file_identities,
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
            staging_dir: None,
        };
        self.jobs.insert(job_id, state);
        self.note_download_activity(job_id);
        self.job_order.push(job_id);
        if let Some(state) = self.jobs.get(&job_id) {
            self.persist_file_identities(job_id, &state.file_identities);
        }
        self.apply_detected_archive_identities(job_id, &detected_archives);
        if !extracted_members.is_empty() {
            self.extracted_members.insert(job_id, extracted_members);
        }
        match self.db.load_failed_extractions(job_id) {
            Ok(failed_members) if !failed_members.is_empty() => {
                self.failed_extractions.insert(job_id, failed_members);
            }
            Ok(_) => {}
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted failed extraction members"
                );
            }
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

        if matches!(status, JobStatus::Repairing) {
            self.metrics.repair_active.fetch_add(1, Ordering::Relaxed);
        }
        if matches!(status, JobStatus::Extracting) {
            self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
        }

        info!(
            job_id = job_id.0,
            committed_count,
            downloaded_bytes,
            restored_download_floor_bytes,
            status = ?status,
            queue_depth,
            "job restored from journal"
        );
        if matches!(
            status,
            JobStatus::Downloading
                | JobStatus::Checking
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        ) {
            self.schedule_job_completion_check(job_id);
        }
        Ok(())
    }
}
