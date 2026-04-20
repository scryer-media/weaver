use super::*;

impl Pipeline {
    pub(crate) fn extraction_staging_dir(&mut self, job_id: JobId) -> PathBuf {
        if let Some(state) = self.jobs.get(&job_id)
            && let Some(ref staging) = state.staging_dir
        {
            return staging.clone();
        }
        let staging = self
            .complete_dir
            .join(".weaver-staging")
            .join(job_id.0.to_string());
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
    }

    pub(crate) fn clear_job_rar_runtime(&mut self, job_id: JobId) {
        self.eagerly_deleted.remove(&job_id);
        self.rar_sets.retain(|(jid, _), _| *jid != job_id);
        self.normalization_retried.remove(&job_id);
    }

    pub(crate) fn set_failed_extraction_member(&mut self, job_id: JobId, member_name: &str) {
        self.failed_extractions
            .entry(job_id)
            .or_default()
            .insert(member_name.to_string());
        let member_owned = member_name.to_string();
        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.add_failed_extraction(job_id, &member_owned) {
                error!(
                    job_id = job_id.0,
                    member = %member_owned,
                    error = %error,
                    "failed to persist failed extraction member"
                );
            }
        });
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
        let members_clone = members.clone();
        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.replace_failed_extractions(job_id, &members_clone) {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to persist failed extraction member set"
                );
            }
        });
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
        &self,
        job_id: JobId,
        set_name: &str,
        volumes: &HashSet<u32>,
    ) {
        if let Err(error) = self
            .db
            .replace_verified_suspect_volumes(job_id, set_name, volumes)
        {
            error!(
                job_id = job_id.0,
                set_name,
                error = %error,
                "failed to persist verified suspect RAR volumes"
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
