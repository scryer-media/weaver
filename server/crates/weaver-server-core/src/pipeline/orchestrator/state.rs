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
        let declared_archive_bytes = state
            .assembly
            .files()
            .filter(|file| archive_sources.contains(&self.current_filename_for_file(job_id, file)))
            .map(|file| file.total_bytes())
            .sum::<u64>()
            .max(1);
        let (initial_entries, initial_bytes) = match ExtractionRoot::snapshot_usage(staging) {
            Ok(usage) => usage,
            Err(error) => {
                let budget = JobExtractionBudget::new(
                    Arc::clone(&self.extraction_limits),
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
        let budget = JobExtractionBudget::new(
            Arc::clone(&self.extraction_limits),
            staging.to_path_buf(),
            declared_archive_bytes,
            initial_entries,
            initial_bytes,
            Arc::clone(&self.metrics),
        )?;
        self.extraction_budgets.insert(job_id, Arc::clone(&budget));
        Ok(budget)
    }

    pub(crate) fn note_write_buffered(&mut self, bytes: usize, segments: usize) {
        self.write_buffered_bytes += bytes;
        self.write_buffered_segments += segments;
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn release_write_buffered(&mut self, bytes: usize, segments: usize) {
        self.write_buffered_bytes = self.write_buffered_bytes.saturating_sub(bytes);
        self.write_buffered_segments = self.write_buffered_segments.saturating_sub(segments);
        self.publish_write_backlog_metrics();
    }

    pub(crate) fn publish_write_backlog_metrics(&self) {
        self.metrics
            .write_buffered_bytes
            .store(self.write_buffered_bytes as u64, Ordering::Relaxed);
        self.metrics
            .write_buffered_segments
            .store(self.write_buffered_segments, Ordering::Relaxed);
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
        for file_id in file_ids {
            if let Some(buf) = self.write_buffers.remove(&file_id) {
                released_bytes += buf.buffered_bytes();
                released_segments += buf.buffered_len();
            }
            // Parked uuencode parts are held in memory for want of an offset,
            // so they have to be released on the same teardown the write buffer
            // is, or a torn-down job keeps its bytes alive.
            if let Some(uu) = self.uu_files.remove(&file_id) {
                released_bytes += uu.parked_bytes();
                released_segments += uu.parked.len();
            }
            self.uu_park_requeues
                .retain(|segment_id, _| segment_id.file_id != file_id);
            self.file_prefix_16k.remove(&file_id);
        }

        if released_bytes > 0 || released_segments > 0 {
            self.release_write_buffered(released_bytes, released_segments);
        }
    }

    pub(crate) fn clear_job_extraction_runtime(&mut self, job_id: JobId) {
        self.extracted_members.remove(&job_id);
        self.extracted_archives.remove(&job_id);
        self.inflight_extractions.remove(&job_id);
        self.failed_extractions.remove(&job_id);
        self.pending_concat.remove(&job_id);
        self.par2_bypassed.remove(&job_id);
        self.par2_verified.remove(&job_id);
        // The verdict that retired them is gone, so a reprocessed job rebuilds
        // its split topologies and asks the recovery set again.
        self.par2_joined_split_sets.remove(&job_id);
        // The verdict is gone, so the post-verdict re-entry budget goes with it.
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            runtime.post_verdict_reconcile_attempts = 0;
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

    pub(crate) fn persist_verified_suspect_volumes(
        &mut self,
        job_id: JobId,
        set_name: &str,
        volumes: &HashSet<u32>,
    ) {
        let key = (job_id, set_name.to_string());
        let mut launch = None;
        {
            let state = self
                .verified_suspect_persist_state
                .entry(key.clone())
                .or_default();
            state.desired = volumes.clone();
            if state.in_flight_version.is_some() {
                state.queued = true;
            } else {
                state.next_version = state.next_version.saturating_add(1);
                let version = state.next_version;
                state.in_flight_version = Some(version);
                state.queued = false;
                launch = Some((version, state.desired.clone()));
            }
        }

        if let Some((version, desired)) = launch {
            self.spawn_verified_suspect_persist(job_id, set_name.to_string(), version, desired);
        }
    }

    fn spawn_verified_suspect_persist(
        &self,
        job_id: JobId,
        set_name: String,
        version: u64,
        volumes: HashSet<u32>,
    ) {
        let done_tx = self.verified_suspect_persist_done_tx.clone();
        tokio::spawn(async move {
            let _ = volumes;
            let result = Ok(());

            let _ = done_tx
                .send(crate::pipeline::VerifiedSuspectPersistDone {
                    job_id,
                    set_name,
                    version,
                    result,
                })
                .await;
        });
    }

    pub(crate) fn handle_verified_suspect_persist_done(
        &mut self,
        done: crate::pipeline::VerifiedSuspectPersistDone,
    ) {
        if let Err(error) = &done.result {
            error!(
                job_id = done.job_id.0,
                set_name = %done.set_name,
                error = %error,
                "verified suspect RAR volume persistence failed"
            );
        }

        let key = (done.job_id, done.set_name.clone());
        let mut relaunch = None;
        let mut remove_entry = false;
        if let Some(state) = self.verified_suspect_persist_state.get_mut(&key) {
            if state.in_flight_version != Some(done.version) {
                return;
            }
            state.in_flight_version = None;

            if state.queued {
                state.queued = false;
                state.next_version = state.next_version.saturating_add(1);
                let version = state.next_version;
                state.in_flight_version = Some(version);
                relaunch = Some((version, state.desired.clone()));
            } else if state.desired.is_empty() {
                remove_entry = true;
            }
        }

        if remove_entry {
            self.verified_suspect_persist_state.remove(&key);
        }
        if let Some((version, desired)) = relaunch {
            self.spawn_verified_suspect_persist(done.job_id, done.set_name, version, desired);
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
