use super::*;

/// Sanitize a job name into a safe directory name.
/// Replaces dangerous characters, truncates to 200 chars, trims trailing dots/spaces.
pub(super) fn sanitize_dirname(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | '<' | '>' | '?' | '*' | '|' | '"' | ':' => '_',
            _ => c,
        })
        .take(200)
        .collect();
    sanitized.trim_end_matches(['.', ' ']).to_string()
}

impl Pipeline {
    /// Compute the per-job working directory under intermediate_dir.
    /// Uses `{sanitized_name}`, with `.#{job_id}` suffix on collision.
    fn compute_working_dir(&self, job_id: JobId, name: &str) -> PathBuf {
        let dir_name = sanitize_dirname(name);
        let candidate = self.intermediate_dir.join(&dir_name);
        if !candidate.exists() {
            candidate
        } else {
            self.intermediate_dir
                .join(format!("{}.#{}", dir_name, job_id.0))
        }
    }

    /// Add a new job and populate the download queue.
    pub(super) async fn add_job(
        &mut self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        let started = std::time::Instant::now();
        if self.jobs.contains_key(&job_id) {
            return Err(weaver_scheduler::SchedulerError::JobExists(job_id));
        }

        // Create per-job working directory.
        let working_dir = self.compute_working_dir(job_id, &spec.name);
        tokio::fs::create_dir_all(&working_dir).await.map_err(|e| {
            weaver_scheduler::SchedulerError::Other(format!(
                "failed to create working dir {}: {e}",
                working_dir.display()
            ))
        })?;
        info!(
            job_id = job_id.0,
            working_dir = %working_dir.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "working_dir_ready",
            "pipeline add_job stage"
        );

        // Persist job creation to SQLite for crash recovery.
        let nzb_hash = {
            use sha2::{Digest, Sha256};
            let bytes = std::fs::read(&nzb_path).unwrap_or_default();
            let hash = Sha256::digest(&bytes);
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&hash);
            arr
        };
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Err(e) = self.db.create_active_job(&weaver_state::ActiveJob {
            job_id,
            nzb_hash,
            nzb_path,
            output_dir: working_dir.clone(),
            created_at,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
        }) {
            error!(error = %e, "db write failed for create_active_job");
        }
        info!(
            job_id = job_id.0,
            elapsed_ms = started.elapsed().as_millis() as u64,
            stage = "active_job_persisted",
            "pipeline add_job stage"
        );

        // Create assembly and populate per-job download queues.
        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &HashSet::new());

        // Check available disk space and warn if insufficient.
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
            created_at_epoch_ms: weaver_scheduler::job::epoch_ms_now(),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            working_dir: working_dir.clone(),
            downloaded_bytes: 0,
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

    /// Build a JobAssembly and populate per-job download queues.
    ///
    /// Segments in `skip` are already committed (from recovery) and are not queued.
    /// Returns (assembly, download_queue, recovery_queue).
    pub(super) fn build_job_assembly(
        job_id: JobId,
        spec: &JobSpec,
        skip: &HashSet<SegmentId>,
    ) -> (JobAssembly, DownloadQueue, DownloadQueue) {
        let mut assembly = JobAssembly::new(job_id);
        let mut download_queue = DownloadQueue::new();
        let mut recovery_queue = DownloadQueue::new();
        let mut has_par2_index = false;
        let mut recovery_files: Vec<(u32, u64)> = Vec::new(); // (file_index, total_bytes)

        for (file_index, file_spec) in spec.files.iter().enumerate() {
            let file_id = NzbFileId {
                job_id,
                file_index: file_index as u32,
            };

            let segment_sizes: Vec<u32> = file_spec.segments.iter().map(|s| s.bytes).collect();

            let mut file_assembly = weaver_assembly::FileAssembly::new(
                file_id,
                file_spec.filename.clone(),
                file_spec.role.clone(),
                segment_sizes,
            );

            if matches!(
                file_spec.role,
                weaver_core::classify::FileRole::Par2 { is_index: true, .. }
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
                        message_id: weaver_core::id::MessageId::new(&seg.message_id),
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

        // If no PAR2 index file exists, promote the smallest recovery volume
        // to the download queue for early verification metadata. Every PAR2 file
        // contains verification checksums; the smallest is essentially the index.
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

    /// Reprocess a failed job: rebuild assembly with all files complete,
    /// reload metadata from disk, and re-run post-processing.
    pub(super) async fn reprocess_job(
        &mut self,
        job_id: JobId,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        // Check if job is in self.jobs (Case A) or only in history (Case B).
        let in_jobs = self.jobs.contains_key(&job_id);

        if in_jobs {
            // Case A: job still in self.jobs — must be Failed.
            let state = self.jobs.get(&job_id).unwrap();
            if !matches!(state.status, JobStatus::Failed { .. }) {
                return Err(weaver_scheduler::SchedulerError::Other(format!(
                    "job {} is not failed",
                    job_id.0
                )));
            }
        } else {
            // Case B: job only in history — rebuild from NZB on disk.
            let history_entry = self.finished_jobs.iter().find(|j| j.job_id == job_id);
            let Some(info) = history_entry else {
                return Err(weaver_scheduler::SchedulerError::JobNotFound(job_id));
            };
            if !matches!(info.status, JobStatus::Failed { .. }) {
                return Err(weaver_scheduler::SchedulerError::Other(format!(
                    "job {} is not failed",
                    job_id.0
                )));
            }

            let nzb_path = self.nzb_dir.join(format!("{}.nzb", job_id.0));
            let raw = tokio::fs::read(&nzb_path).await.map_err(|e| {
                weaver_scheduler::SchedulerError::Other(format!(
                    "failed to read NZB for job {}: {e}",
                    job_id.0
                ))
            })?;
            let nzb_bytes = crate::decompress_nzb(&raw);
            let nzb = weaver_nzb::parse_nzb(&nzb_bytes).map_err(|e| {
                weaver_scheduler::SchedulerError::Other(format!("failed to parse NZB: {e}"))
            })?;

            let spec = crate::import::nzb_to_spec(
                &nzb,
                &nzb_path,
                info.category.clone(),
                info.metadata.clone(),
            );

            // Reconstruct working_dir from history output_dir, or compute a new one.
            let working_dir = info
                .output_dir
                .as_ref()
                .map(PathBuf::from)
                .unwrap_or_else(|| self.compute_working_dir(job_id, &spec.name));

            // Build assembly with ALL segments in skip set (everything already on disk).
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
                created_at_epoch_ms: weaver_scheduler::job::epoch_ms_now(),
                queued_repair_at_epoch_ms: None,
                queued_extract_at_epoch_ms: None,
                paused_resume_status: None,
                working_dir,
                downloaded_bytes: info.downloaded_bytes,
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
        }

        // --- Common path for both cases ---

        // For Case A, rebuild the assembly with all segments marked complete.
        if in_jobs {
            let state = self.jobs.get(&job_id).unwrap();
            let all_segments = Self::all_segment_ids(job_id, &state.spec);
            // Need to clone spec to avoid borrow conflict.
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
        }

        // Remove old history entry.
        self.finished_jobs.retain(|j| j.job_id != job_id);
        let db = self.db.clone();
        let jid = job_id.0;
        tokio::task::spawn_blocking(move || {
            let _ = db.delete_job_history(jid);
        });

        // Clear stale per-job caches from previous attempt.
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

        // Add to job_order so it shows as active.
        if !self.job_order.contains(&job_id) {
            self.job_order.push(job_id);
        }

        let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });

        info!(job_id = job_id.0, "reprocessing failed job");

        // Reload metadata from disk files, then trigger post-processing.
        self.reload_metadata_from_disk(job_id).await;
        self.check_job_completion(job_id).await;

        Ok(())
    }

    /// Reload PAR2 and RAR metadata from on-disk files for a reprocessed job.
    async fn restore_par2_state_from_disk(&mut self, job_id: JobId) {
        // Collect file IDs and roles to avoid borrow conflicts.
        let files: Vec<(NzbFileId, weaver_core::classify::FileRole)> = {
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

        // Load PAR2 index files first (needed before recovery volumes).
        for (file_id, role) in &files {
            if matches!(
                role,
                weaver_core::classify::FileRole::Par2 { is_index: true, .. }
            ) {
                self.try_load_par2_metadata(job_id, *file_id).await;
            }
        }

        // Merge PAR2 recovery volumes.
        for (file_id, role) in &files {
            if matches!(
                role,
                weaver_core::classify::FileRole::Par2 {
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

    /// Reload PAR2 and RAR metadata from on-disk files for a reprocessed job.
    async fn reload_metadata_from_disk(&mut self, job_id: JobId) {
        self.restore_par2_state_from_disk(job_id).await;

        // Collect file IDs and roles to avoid borrow conflicts.
        let files: Vec<(NzbFileId, weaver_core::classify::FileRole)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .files()
                .map(|f| (f.file_id(), f.role().clone()))
                .collect()
        };

        // Load archive topology from RAR volume 0.
        for (file_id, role) in &files {
            if matches!(role, weaver_core::classify::FileRole::RarVolume { .. }) {
                self.try_update_archive_topology(job_id, *file_id).await;
            }
        }

        // Load non-RAR archive topology from completed files.
        for (file_id, role) in &files {
            if matches!(
                role,
                weaver_core::classify::FileRole::SevenZipArchive
                    | weaver_core::classify::FileRole::SevenZipSplit { .. }
                    | weaver_core::classify::FileRole::ZipArchive
                    | weaver_core::classify::FileRole::TarArchive
                    | weaver_core::classify::FileRole::TarGzArchive
                    | weaver_core::classify::FileRole::GzArchive
                    | weaver_core::classify::FileRole::DeflateArchive
                    | weaver_core::classify::FileRole::BrotliArchive
                    | weaver_core::classify::FileRole::ZstdArchive
                    | weaver_core::classify::FileRole::Bzip2Archive
                    | weaver_core::classify::FileRole::SplitFile { .. }
            ) {
                self.try_update_7z_topology(job_id, *file_id);
            }
        }
    }

    /// Collect all segment IDs for a job spec (used to mark everything as "already downloaded").
    pub(super) fn all_segment_ids(job_id: JobId, spec: &JobSpec) -> HashSet<SegmentId> {
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

    /// Restore a job from crash-recovery journal.
    pub(super) async fn restore_job(
        &mut self,
        job_id: JobId,
        spec: JobSpec,
        committed_segments: HashSet<SegmentId>,
        extracted_members: HashSet<String>,
        status: JobStatus,
        working_dir: PathBuf,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        if self.jobs.contains_key(&job_id) {
            return Err(weaver_scheduler::SchedulerError::JobExists(job_id));
        }

        let committed_count = committed_segments.len();
        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &committed_segments);

        // Compute downloaded bytes from committed segments.
        let committed_ref = &committed_segments;
        let downloaded_bytes: u64 = spec
            .files
            .iter()
            .enumerate()
            .flat_map(|(fi, file_spec)| {
                let file_id = NzbFileId {
                    job_id,
                    file_index: fi as u32,
                };
                file_spec.segments.iter().filter_map(move |seg| {
                    let sid = SegmentId {
                        file_id,
                        segment_number: seg.number,
                    };
                    if committed_ref.contains(&sid) {
                        Some(seg.bytes as u64)
                    } else {
                        None
                    }
                })
            })
            .sum();

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
            created_at_epoch_ms: weaver_scheduler::job::epoch_ms_now(),
            queued_repair_at_epoch_ms: matches!(status, JobStatus::QueuedRepair)
                .then(weaver_scheduler::job::epoch_ms_now),
            queued_extract_at_epoch_ms: matches!(status, JobStatus::QueuedExtract)
                .then(weaver_scheduler::job::epoch_ms_now),
            paused_resume_status: None,
            working_dir,
            downloaded_bytes,
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

        info!(
            job_id = job_id.0,
            committed_count,
            downloaded_bytes,
            status = ?status,
            queue_depth,
            "job restored from journal"
        );
        if matches!(
            status,
            JobStatus::Downloading
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::QueuedExtract
        ) {
            self.schedule_job_completion_check(job_id);
        }
        Ok(())
    }
}
