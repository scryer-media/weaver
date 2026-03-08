use super::*;

impl Pipeline {
    /// Add a new job and populate the download queue.
    pub(super) async fn add_job(
        &mut self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        if self.jobs.contains_key(&job_id) {
            return Err(weaver_scheduler::SchedulerError::JobExists(job_id));
        }

        // Write JobCreated to the journal for crash recovery.
        let nzb_hash = {
            use sha2::{Sha256, Digest};
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
        if let Err(e) = self.journal.append(&JournalEntry::JobCreated {
            job_id,
            nzb_hash,
            nzb_path,
            output_dir: self.output_dir.clone(),
            created_at,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
        }).await {
            error!(error = %e, "journal write failed for JobCreated");
        }

        // Create assembly and populate per-job download queues.
        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &HashSet::new());

        // Check available disk space and warn if insufficient.
        check_disk_space(&self.output_dir, spec.total_bytes);

        let queue_depth = download_queue.len() + recovery_queue.len();

        let _ = self.event_tx.send(PipelineEvent::JobCreated {
            job_id,
            name: spec.name.clone(),
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
        });

        let state = JobState {
            job_id,
            spec,
            status: JobStatus::Downloading,
            assembly,
            created_at: std::time::Instant::now(),
            downloaded_bytes: 0,
            failed_bytes: 0,
            health_probing: false,
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
        };
        self.jobs.insert(job_id, state);
        self.job_order.push(job_id);

        info!(
            job_id = job_id.0,
            queue_depth,
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

            let priority = file_spec.role.download_priority();
            let is_recovery = file_spec.role.is_recovery();
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
                    let _ = file_assembly.commit_segment_meta(seg.number, seg.bytes);
                } else {
                    target_queue.push(DownloadWork {
                        segment_id,
                        message_id: weaver_core::id::MessageId::new(&seg.message_id),
                        groups: file_spec.groups.clone(),
                        priority,
                        byte_estimate: seg.bytes,
                        retry_count: 0,
                        is_recovery,
                    });
                }
            }

            assembly.add_file(file_assembly);
        }

        (assembly, download_queue, recovery_queue)
    }

    /// Restore a job from crash-recovery journal.
    pub(super) fn restore_job(
        &mut self,
        job_id: JobId,
        spec: JobSpec,
        committed_segments: HashSet<SegmentId>,
        status: JobStatus,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        if self.jobs.contains_key(&job_id) {
            return Err(weaver_scheduler::SchedulerError::JobExists(job_id));
        }

        let committed_count = committed_segments.len();
        let (assembly, download_queue, recovery_queue) =
            Self::build_job_assembly(job_id, &spec, &committed_segments);

        // Compute downloaded bytes from committed segments.
        let committed_ref = &committed_segments;
        let downloaded_bytes: u64 = spec.files.iter().enumerate().flat_map(|(fi, file_spec)| {
            let file_id = NzbFileId { job_id, file_index: fi as u32 };
            file_spec.segments.iter().filter_map(move |seg| {
                let sid = SegmentId { file_id, segment_number: seg.number };
                if committed_ref.contains(&sid) { Some(seg.bytes as u64) } else { None }
            })
        }).sum();

        let queue_depth = download_queue.len() + recovery_queue.len();

        let _ = self.event_tx.send(PipelineEvent::JobCreated {
            job_id,
            name: spec.name.clone(),
            total_files: spec.files.len() as u32,
            total_bytes: spec.total_bytes,
        });

        let state = JobState {
            job_id,
            spec,
            status: status.clone(),
            assembly,
            created_at: std::time::Instant::now(),
            downloaded_bytes,
            failed_bytes: 0,
            health_probing: false,
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
        };
        self.jobs.insert(job_id, state);
        self.job_order.push(job_id);

        info!(
            job_id = job_id.0,
            committed_count,
            downloaded_bytes,
            status = ?status,
            queue_depth,
            "job restored from journal"
        );
        Ok(())
    }
}
