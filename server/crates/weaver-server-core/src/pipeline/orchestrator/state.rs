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
        let file_ids: Vec<NzbFileId> = self
            .write_buffers
            .keys()
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
    }

    pub(crate) fn clear_job_rar_runtime(&mut self, job_id: JobId) {
        self.eagerly_deleted.remove(&job_id);
        self.rar_sets.retain(|(jid, _), _| *jid != job_id);
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
