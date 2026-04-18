use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use tracing::{error, info};

use crate::events::model::PipelineEvent;
use crate::jobs::assembly::JobAssembly;
use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};
use crate::jobs::model::{JobSpec, JobState, JobStatus};
use crate::jobs::working_dir::{compute_working_dir, working_dir_marker_path};
use crate::pipeline::{Pipeline, check_disk_space};
use crate::{DownloadQueue, DownloadWork, RestoreJobRequest};

impl Pipeline {
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
            let history_entry = self.finished_jobs.iter().find(|j| j.job_id == job_id);
            let Some(info) = history_entry else {
                return Err(crate::SchedulerError::JobNotFound(job_id));
            };
            if !matches!(info.status, JobStatus::Failed { .. }) {
                return Err(crate::SchedulerError::Conflict(format!(
                    "job {} is not failed",
                    job_id.0
                )));
            }

            let nzb_path = self.nzb_dir.join(format!("{}.nzb", job_id.0));
            let nzb = crate::ingest::parse_persisted_nzb(&nzb_path).map_err(|e| match e {
                crate::ingest::PersistedNzbError::Io(error) => {
                    crate::SchedulerError::Io(std::io::Error::new(
                        error.kind(),
                        format!("failed to read NZB for job {}: {error}", job_id.0),
                    ))
                }
                crate::ingest::PersistedNzbError::Parse(error) => crate::SchedulerError::Internal(
                    format!("failed to parse NZB for job {}: {error}", job_id.0),
                ),
            })?;

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
                tracing::warn!(
                    job_id = job_id.0,
                    dir = %working_dir.display(),
                    error = %error,
                    "failed to stamp reprocessed working directory as Weaver-owned"
                );
            }

            let all_segments = Self::all_segment_ids(job_id, &spec);
            let (assembly, download_queue, recovery_queue) =
                Self::build_job_assembly(job_id, &spec, &all_segments);

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
                held_segments: Vec::new(),
                download_queue,
                recovery_queue,
                staging_dir: None,
            };
            self.jobs.insert(job_id, state);
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
            state.download_queue = download_queue;
            state.recovery_queue = recovery_queue;
            state.held_segments.clear();
            state.failed_bytes = 0;
            self.note_download_activity(job_id);
        }

        self.finished_jobs.retain(|j| j.job_id != job_id);
        let db = self.db.clone();
        let jid = job_id.0;
        tokio::task::spawn_blocking(move || {
            let _ = db.delete_job_history(jid);
        });

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
                "failed to clear persisted verified suspect RAR volumes during reprocess"
            );
        }

        if !self.job_order.contains(&job_id) {
            self.job_order.push(job_id);
        }

        let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });

        info!(job_id = job_id.0, "reprocessing failed job");

        self.reload_metadata_from_disk(job_id).await;
        self.check_job_completion(job_id).await;

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

        for (file_id, role) in &files {
            if matches!(role, weaver_model::files::FileRole::RarVolume { .. }) {
                self.try_update_archive_topology(job_id, *file_id).await;
            }
        }

        for (file_id, role) in &files {
            if matches!(
                role,
                weaver_model::files::FileRole::SevenZipArchive
                    | weaver_model::files::FileRole::SevenZipSplit { .. }
                    | weaver_model::files::FileRole::ZipArchive
                    | weaver_model::files::FileRole::TarArchive
                    | weaver_model::files::FileRole::TarGzArchive
                    | weaver_model::files::FileRole::TarBz2Archive
                    | weaver_model::files::FileRole::GzArchive
                    | weaver_model::files::FileRole::DeflateArchive
                    | weaver_model::files::FileRole::BrotliArchive
                    | weaver_model::files::FileRole::ZstdArchive
                    | weaver_model::files::FileRole::Bzip2Archive
                    | weaver_model::files::FileRole::SplitFile { .. }
            ) {
                self.try_update_7z_topology(job_id, *file_id);
            }
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
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
            staging_dir: None,
        };
        self.jobs.insert(job_id, state);
        self.note_download_activity(job_id);
        self.job_order.push(job_id);
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
        self.restore_par2_state_from_disk(job_id).await;
        self.restore_rar_state_for_job(job_id).await;

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
