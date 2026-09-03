use super::*;

impl Pipeline {
    pub(crate) fn deterministic_extraction_staging_dir(&self, job_id: JobId) -> PathBuf {
        self.complete_dir
            .join(".weaver-staging")
            .join(job_id.0.to_string())
    }

    pub(crate) fn extraction_staging_dir(&mut self, job_id: JobId) -> PathBuf {
        if let Some(state) = self.jobs.get(&job_id)
            && let Some(ref staging) = state.staging_dir
        {
            return staging.clone();
        }
        let staging = self.deterministic_extraction_staging_dir(job_id);
        if let Err(e) = std::fs::create_dir_all(&staging) {
            tracing::warn!(
                job_id = job_id.0,
                path = %staging.display(),
                error = %e,
                "failed to create staging dir"
            );
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.staging_dir = Some(staging.clone());
        }
        staging
    }

    pub(crate) fn extraction_budget(
        &mut self,
        job_id: JobId,
        staging: &std::path::Path,
    ) -> Result<Arc<JobExtractionBudget>, String> {
        if let Some(budget) = self.extraction_budgets.get(&job_id) {
            return Ok(Arc::clone(budget));
        }
        let state = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| format!("job {job_id:?} not found"))?;
        let archive_sources = state
            .assembly
            .archive_topologies()
            .values()
            .flat_map(|topology| topology.volume_map.keys().cloned())
            .collect::<HashSet<_>>();
        // A direct set's source volumes never enter the archive topology —
        // that is the point of the route — but they are archive input all the
        // same, and their members land in the staging tree this budget is
        // about to snapshot. Leaving them out collapses the ratio base to its
        // floor and then counts the set's own finalized output as a
        // violation of it.
        let direct_source_files: HashSet<u32> = self
            .direct_store
            .sets_for(job_id)
            .iter()
            .flat_map(|set| set.plan().files.keys().copied())
            .collect();
        let declared_archive_bytes = state
            .assembly
            .files()
            .filter(|file| {
                archive_sources.contains(&self.current_filename_for_file(job_id, file))
                    || direct_source_files.contains(&file.file_id().file_index)
            })
            .map(|file| file.total_bytes())
            .sum::<u64>()
            .max(1);
        let (initial_entries, initial_bytes) = match ExtractionRoot::snapshot_usage(staging) {
            Ok(usage) => usage,
            Err(error) => {
                let budget = JobExtractionBudget::new_with_process_memory(
                    Arc::clone(&self.extraction_limits),
                    Arc::clone(&self.process_memory_budget),
                    staging.to_path_buf(),
                    declared_archive_bytes,
                    0,
                    0,
                    Arc::clone(&self.metrics),
                )?;
                let rejection = if error.contains("symlink") || error.contains("reparse") {
                    budget.reject_unsafe_path(error)
                } else {
                    budget.reject_unsupported_entry(error)
                };
                return Err(rejection);
            }
        };
        let budget = JobExtractionBudget::new_with_process_memory(
            Arc::clone(&self.extraction_limits),
            Arc::clone(&self.process_memory_budget),
            staging.to_path_buf(),
            declared_archive_bytes,
            initial_entries,
            initial_bytes,
            Arc::clone(&self.metrics),
        )?;
        self.extraction_budgets.insert(job_id, Arc::clone(&budget));
        let cancellation_budget = Arc::clone(&budget);
        self.shared_state
            .register_job_cancellation(job_id, Arc::new(move || cancellation_budget.cancel()));
        Ok(budget)
    }

    pub(crate) fn note_write_buffered(&mut self, bytes: usize, segments: usize) {
        self.write_buffered_bytes = self.write_buffered_bytes.saturating_add(bytes);
        self.write_buffered_segments = self.write_buffered_segments.saturating_add(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn release_write_buffered(&mut self, bytes: usize, segments: usize) {
        debug_assert!(
            self.write_buffered_bytes >= bytes,
            "resident write backlog over-release: have {}, releasing {bytes}",
            self.write_buffered_bytes
        );
        debug_assert!(
            self.write_buffered_segments >= segments,
            "resident write segment backlog over-release: have {}, releasing {segments}",
            self.write_buffered_segments
        );
        self.write_buffered_bytes = self.write_buffered_bytes.saturating_sub(bytes);
        self.write_buffered_segments = self.write_buffered_segments.saturating_sub(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn note_uu_spooled(&mut self, bytes: usize, segments: usize) {
        self.uu_spooled_bytes = self.uu_spooled_bytes.saturating_add(bytes);
        self.uu_spooled_segments = self.uu_spooled_segments.saturating_add(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn release_uu_spooled(&mut self, bytes: usize, segments: usize) {
        debug_assert!(
            self.uu_spooled_bytes >= bytes,
            "UU spool backlog over-release: have {}, releasing {bytes}",
            self.uu_spooled_bytes
        );
        debug_assert!(
            self.uu_spooled_segments >= segments,
            "UU spool segment backlog over-release: have {}, releasing {segments}",
            self.uu_spooled_segments
        );
        self.uu_spooled_bytes = self.uu_spooled_bytes.saturating_sub(bytes);
        self.uu_spooled_segments = self.uu_spooled_segments.saturating_sub(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn note_uu_parked_segment(&mut self) {
        self.uu_parked_segments = self.uu_parked_segments.saturating_add(1);
    }

    pub(crate) fn release_uu_parked_segment(&mut self) {
        debug_assert!(
            self.uu_parked_segments > 0,
            "UU parked segment over-release"
        );
        self.uu_parked_segments = self.uu_parked_segments.saturating_sub(1);
    }

    pub(crate) fn publish_write_backlog_metrics(&self) {
        self.metrics
            .write_buffered_bytes
            .store(self.write_buffered_bytes as u64, Ordering::Relaxed);
        self.metrics
            .write_buffered_segments
            .store(self.write_buffered_segments, Ordering::Relaxed);
        self.metrics.write_pending_bytes.store(
            self.write_buffered_bytes
                .saturating_add(self.uu_spooled_bytes) as u64,
            Ordering::Relaxed,
        );
        self.metrics
            .uu_spooled_bytes
            .store(self.uu_spooled_bytes as u64, Ordering::Relaxed);
        self.metrics
            .uu_spooled_segments
            .store(self.uu_spooled_segments, Ordering::Relaxed);
    }

    pub(crate) fn publish_active_stage_metrics(&self) {
        self.metrics
            .active_downloads
            .store(self.active_downloads, Ordering::Relaxed);
        self.metrics.active_decodes.store(
            self.active_decodes_by_job.values().sum::<usize>(),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn clear_job_write_backlog(&mut self, job_id: JobId) {
        // Both maps, not just the write buffers: a completed uuencode file
        // keeps a tombstone entry in `uu_files` after its write buffer is gone
        // (it is what suppresses the restart checkpoint), and teardown is where
        // that entry is finally dropped.
        let file_ids: std::collections::HashSet<NzbFileId> = self
            .write_buffers
            .keys()
            .chain(self.uu_files.keys())
            .copied()
            .filter(|file_id| file_id.job_id == job_id)
            .collect();

        let mut released_bytes = 0usize;
        let mut released_segments = 0usize;
        let mut released_spooled_bytes = 0usize;
        let mut released_spooled_segments = 0usize;
        let mut released_uu_parked_segments = 0usize;
        for file_id in file_ids {
            if let Some(buf) = self.write_buffers.remove(&file_id) {
                released_bytes += buf.buffered_bytes();
                released_segments += buf.buffered_len();
            }
            // Memory and disk parks use distinct ledgers. Removing the entry
            // drops `TempPath` and unlinks every remaining per-job spool file.
            if let Some(uu) = self.uu_files.remove(&file_id) {
                released_bytes += uu.parked_memory_bytes();
                released_segments += uu.parked.len() - uu.parked_spooled_segments();
                released_spooled_bytes += uu.parked_spooled_bytes();
                released_spooled_segments += uu.parked_spooled_segments();
                released_uu_parked_segments += uu.parked.len();
            }
            self.uu_park_requeues
                .retain(|segment_id, _| segment_id.file_id != file_id);
            self.file_prefix_16k.remove(&file_id);
            self.file_declared_size.remove(&file_id);
        }

        if released_bytes > 0 || released_segments > 0 {
            self.release_write_buffered(released_bytes, released_segments);
        }
        if released_spooled_bytes > 0 || released_spooled_segments > 0 {
            self.release_uu_spooled(released_spooled_bytes, released_spooled_segments);
        }
        for _ in 0..released_uu_parked_segments {
            self.release_uu_parked_segment();
        }
        remove_empty_uu_park_dir(&self.uu_spool_root, job_id);
    }

    pub(crate) fn clear_job_extraction_runtime(&mut self, job_id: JobId) {
        self.extracted_members.remove(&job_id);
        self.extracted_archives.remove(&job_id);
        self.missing_volume_archive_sets.remove(&job_id);
        self.inflight_extractions.remove(&job_id);
        self.failed_extractions.remove(&job_id);
        self.pending_concat.remove(&job_id);
        self.par2_bypassed.remove(&job_id);
        self.par2_verified.remove(&job_id);
        // The verdict that retired them is gone, so a reprocessed job rebuilds
        // its split topologies and asks the recovery set again.
        self.par2_joined_split_sets.remove(&job_id);
        // The verdict is gone, so the post-verdict re-entry budget goes with it.
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id)
            && let Some(set_runtime) = runtime.served_mut()
        {
            set_runtime.post_verdict_reconcile_attempts = 0;
        }
        self.par2_pre_repair_dir_entries.remove(&job_id);
        self.sfv_checked.remove(&job_id);
    }

    pub(crate) fn clear_job_rar_runtime(&mut self, job_id: JobId) {
        self.eagerly_deleted.remove(&job_id);
        self.rar_sets.retain(|(jid, _), _| *jid != job_id);
        self.clear_rar_unlock_priorities(job_id);
        self.pending_rar_capacity_retries
            .retain(|(jid, _, _)| *jid != job_id);
        self.rar_waiting_members
            .retain(|(jid, _, _), _| *jid != job_id);
        self.normalization_retried.remove(&job_id);
    }

    pub(crate) fn set_failed_extraction_member(&mut self, job_id: JobId, member_name: &str) {
        self.failed_extractions
            .entry(job_id)
            .or_default()
            .insert(member_name.to_string());
    }

    pub(crate) fn replace_failed_extraction_members(
        &mut self,
        job_id: JobId,
        members: HashSet<String>,
    ) {
        if members.is_empty() {
            self.failed_extractions.remove(&job_id);
        } else {
            self.failed_extractions.insert(job_id, members.clone());
        }
    }

    pub(crate) fn set_normalization_retried_state(
        &mut self,
        job_id: JobId,
        normalization_retried: bool,
    ) {
        if normalization_retried {
            self.normalization_retried.insert(job_id);
        } else {
            self.normalization_retried.remove(&job_id);
        }
        if let Err(error) = self
            .db
            .set_active_job_normalization_retried(job_id, normalization_retried)
        {
            error!(
                job_id = job_id.0,
                normalization_retried,
                error = %error,
                "failed to persist normalization retry state"
            );
        }
    }

    pub(crate) fn write_target_for_file(
        &self,
        file_id: NzbFileId,
    ) -> Option<(JobId, String, PathBuf, PathBuf)> {
        let job_id = file_id.job_id;
        let state = self.jobs.get(&job_id)?;
        let file_asm = state.assembly.file(file_id)?;
        let filename = self.current_filename_for_file(job_id, file_asm);
        let working_dir = state.working_dir.clone();
        let file_path = working_dir.join(&filename);
        Some((job_id, filename, working_dir, file_path))
    }
}
