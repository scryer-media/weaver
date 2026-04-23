use super::*;

pub(crate) struct SharedOutputFile {
    pub(crate) inner: std::io::BufWriter<std::fs::File>,
}

pub(crate) struct DirectOutputWriter {
    pub(crate) shared: Option<Rc<RefCell<SharedOutputFile>>>,
    pub(crate) bytes_written: u64,
    pub(crate) volume_index: u32,
    pub(crate) checkpoint: Option<Arc<ExtractionCheckpointState>>,
}

impl std::io::Write for DirectOutputWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(shared) = &self.shared {
            let written = shared.borrow_mut().inner.write(buf)?;
            self.bytes_written += written as u64;
            Ok(written)
        } else {
            self.bytes_written += buf.len() as u64;
            Ok(buf.len())
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(shared) = &self.shared {
            shared.borrow_mut().inner.flush()
        } else {
            Ok(())
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        if let Some(shared) = &self.shared {
            shared.borrow_mut().inner.write_all(buf)?;
        }
        self.bytes_written += buf.len() as u64;
        Ok(())
    }
}

impl Drop for DirectOutputWriter {
    fn drop(&mut self) {
        if let Some(shared) = &self.shared {
            let _ = shared.borrow_mut().inner.flush();
        }
        if let Some(checkpoint) = &self.checkpoint
            && let Err(error) = checkpoint.persist_volume(self.volume_index, self.bytes_written)
        {
            checkpoint.record_error(error);
        }
    }
}

pub(crate) struct ExtractionCheckpointState {
    pub(crate) db: crate::Database,
    pub(crate) job_id: JobId,
    pub(crate) set_name: String,
    pub(crate) member_name: String,
    pub(crate) temp_path: String,
    pub(crate) manifest: Mutex<Vec<crate::ExtractionChunk>>,
    pub(crate) next_offset: AtomicU64,
    pub(crate) error: Mutex<Option<String>>,
}

impl ExtractionCheckpointState {
    fn persist_volume(&self, volume_index: u32, bytes_written: u64) -> Result<(), String> {
        if bytes_written == 0 {
            return Ok(());
        }

        let start_offset = self.next_offset.fetch_add(bytes_written, Ordering::SeqCst);
        let end_offset = start_offset + bytes_written;
        let mut manifest = self
            .manifest
            .lock()
            .map_err(|_| "checkpoint manifest poisoned".to_string())?;
        manifest.push(crate::ExtractionChunk {
            member_name: self.member_name.clone(),
            volume_index,
            bytes_written,
            temp_path: self.temp_path.clone(),
            start_offset,
            end_offset,
            verified: true,
            appended: false,
        });
        manifest.sort_by_key(|chunk| (chunk.start_offset, chunk.volume_index));
        self.db
            .replace_member_chunks(self.job_id, &self.set_name, &self.member_name, &manifest)
            .map_err(|error| format!("failed to persist extraction chunks: {error}"))?;
        crate::e2e_failpoint::maybe_trip("extract.after_volume_checkpoint");
        Ok(())
    }

    fn record_error(&self, error: String) {
        if let Ok(mut slot) = self.error.lock()
            && slot.is_none()
        {
            *slot = Some(error);
        }
    }

    pub(crate) fn take_error(&self) -> Option<String> {
        self.error.lock().ok().and_then(|mut slot| slot.take())
    }
}

pub(crate) struct FinalizeMemberContext<'a> {
    pub(crate) db: &'a crate::Database,
    pub(crate) event_tx: &'a broadcast::Sender<PipelineEvent>,
    pub(crate) job_id: JobId,
    pub(crate) set_name: &'a str,
    pub(crate) member_name: &'a str,
    pub(crate) partial_path: &'a std::path::Path,
    pub(crate) out_path: &'a std::path::Path,
    pub(crate) chunk_dir: &'a std::path::Path,
}

#[derive(Debug)]
pub(crate) struct ValidatedExtractionCheckpoint {
    pub(crate) manifest: Vec<crate::ExtractionChunk>,
    pub(crate) completed_volumes: HashSet<u32>,
    pub(crate) next_offset: u64,
}

#[derive(Debug)]
pub(crate) struct ValidatedExtractionManifest {
    pub(crate) manifest: Vec<crate::ExtractionChunk>,
    pub(crate) completed_volumes: HashSet<u32>,
    pub(crate) next_offset: u64,
}

impl Pipeline {
    pub(crate) fn extraction_chunk_root(output_dir: &std::path::Path) -> PathBuf {
        output_dir.join(".weaver-chunks")
    }

    pub(crate) fn member_output_paths(
        output_dir: &std::path::Path,
        member_name: &str,
    ) -> (PathBuf, PathBuf) {
        let out_path = output_dir.join(member_name);
        let partial_path = if let Some(ext) = out_path.extension() {
            out_path.with_extension(format!("{}.partial", ext.to_string_lossy()))
        } else {
            out_path.with_extension("partial")
        };
        (out_path, partial_path)
    }

    pub(crate) fn member_chunk_dir(
        output_dir: &std::path::Path,
        set_name: &str,
        member_name: &str,
    ) -> PathBuf {
        Self::extraction_chunk_root(output_dir)
            .join(crate::jobs::working_dir::sanitize_dirname(set_name))
            .join(member_name)
    }

    pub(crate) fn clear_member_extraction_artifacts(
        db: &crate::Database,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        partial_path: &std::path::Path,
        chunk_dir: &std::path::Path,
    ) -> Result<(), String> {
        let existing = db
            .get_extraction_chunks(job_id, set_name)
            .map_err(|e| format!("failed to load existing extraction chunks: {e}"))?;
        for chunk in existing
            .into_iter()
            .filter(|chunk| chunk.member_name == member_name)
        {
            match std::fs::remove_file(&chunk.temp_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "failed to remove stale extraction chunk {}: {error}",
                        chunk.temp_path
                    ));
                }
            }
        }
        match std::fs::remove_file(partial_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to remove stale partial output {}: {error}",
                    partial_path.display()
                ));
            }
        }
        db.clear_member_chunks(job_id, set_name, member_name)
            .map_err(|e| format!("failed to clear extraction chunk rows: {e}"))?;
        match std::fs::remove_dir_all(chunk_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to remove stale extraction chunk dir {}: {error}",
                    chunk_dir.display()
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_member_extraction_manifest(
        chunks: &[crate::ExtractionChunk],
        first_volume: u32,
        last_volume_index: u32,
    ) -> Result<Option<ValidatedExtractionManifest>, String> {
        if chunks.is_empty() {
            return Ok(None);
        }

        let mut manifest = chunks.to_vec();
        manifest.sort_by_key(|chunk| (chunk.start_offset, chunk.volume_index));

        let temp_path = &manifest[0].temp_path;
        if manifest.iter().any(|chunk| chunk.temp_path != *temp_path) {
            return Err("checkpoint manifest references multiple temp paths".to_string());
        }

        let mut expected_start = 0u64;
        let mut previous_volume = None;
        let mut completed_volumes = HashSet::new();
        for chunk in &manifest {
            if chunk.bytes_written == 0 {
                return Err(format!(
                    "checkpoint chunk for volume {} recorded zero bytes",
                    chunk.volume_index
                ));
            }
            if chunk.start_offset != expected_start {
                return Err(format!(
                    "checkpoint chunk for volume {} starts at {} but expected {}",
                    chunk.volume_index, chunk.start_offset, expected_start
                ));
            }
            if chunk.end_offset != chunk.start_offset + chunk.bytes_written {
                return Err(format!(
                    "checkpoint chunk for volume {} has inconsistent end offset",
                    chunk.volume_index
                ));
            }
            if chunk.volume_index < first_volume || chunk.volume_index > last_volume_index {
                return Err(format!(
                    "checkpoint chunk volume {} outside member range {}..={}",
                    chunk.volume_index, first_volume, last_volume_index
                ));
            }
            if let Some(previous) = previous_volume
                && chunk.volume_index <= previous
            {
                return Err(format!(
                    "checkpoint chunk volumes are not strictly increasing ({} then {})",
                    previous, chunk.volume_index
                ));
            }
            expected_start = chunk.end_offset;
            previous_volume = Some(chunk.volume_index);
            completed_volumes.insert(chunk.volume_index);
        }

        Ok(Some(ValidatedExtractionManifest {
            manifest,
            completed_volumes,
            next_offset: expected_start,
        }))
    }

    pub(crate) fn validate_member_extraction_checkpoint(
        chunks: &[crate::ExtractionChunk],
        partial_path: &std::path::Path,
        first_volume: u32,
        last_volume_index: u32,
    ) -> Result<Option<ValidatedExtractionCheckpoint>, String> {
        let Some(validated) =
            Self::validate_member_extraction_manifest(chunks, first_volume, last_volume_index)?
        else {
            return Ok(None);
        };

        let temp_path = std::path::PathBuf::from(&validated.manifest[0].temp_path);
        if temp_path != partial_path {
            return Err(format!(
                "checkpoint temp path {} does not match expected {}",
                temp_path.display(),
                partial_path.display()
            ));
        }

        let partial_size = std::fs::metadata(partial_path)
            .map_err(|error| {
                format!(
                    "failed to stat checkpoint partial output {}: {error}",
                    partial_path.display()
                )
            })?
            .len();
        if partial_size != validated.next_offset {
            return Err(format!(
                "checkpoint partial output {} has size {} but manifest ends at {}",
                partial_path.display(),
                partial_size,
                validated.next_offset
            ));
        }

        Ok(Some(ValidatedExtractionCheckpoint {
            manifest: validated.manifest,
            completed_volumes: validated.completed_volumes,
            next_offset: validated.next_offset,
        }))
    }

    pub(crate) fn finalize_member_output(ctx: FinalizeMemberContext<'_>) -> Result<u64, String> {
        let FinalizeMemberContext {
            db,
            event_tx,
            job_id,
            set_name,
            member_name,
            partial_path,
            out_path,
            chunk_dir,
        } = ctx;
        let _ = event_tx.send(PipelineEvent::ExtractionMemberAppendStarted {
            job_id,
            set_name: set_name.to_string(),
            member: member_name.to_string(),
        });

        match std::fs::remove_file(out_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to remove stale finalized output {}: {error}",
                    out_path.display()
                ));
            }
        }

        let size = std::fs::metadata(partial_path)
            .map_err(|e| {
                format!(
                    "failed to stat partial output {}: {e}",
                    partial_path.display()
                )
            })?
            .len();
        std::fs::rename(partial_path, out_path)
            .map_err(|e| format!("failed to finalize output {}: {e}", out_path.display()))?;
        crate::e2e_failpoint::maybe_trip("extract.after_finalize_rename_before_record");

        match std::fs::remove_dir_all(chunk_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    path = %chunk_dir.display(),
                    error = %error,
                    "failed to remove legacy chunk dir during finalize"
                );
            }
        }

        if let Err(error) = db.clear_member_chunks(job_id, set_name, member_name) {
            warn!(
                job_id = job_id.0,
                set_name,
                member = member_name,
                error = %error,
                "failed to clear extraction checkpoint manifest after finalize"
            );
        }

        let _ = event_tx.send(PipelineEvent::ExtractionMemberAppendFinished {
            job_id,
            set_name: set_name.to_string(),
            member: member_name.to_string(),
        });

        Ok(size)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "thin wrapper around finalized checkpoint path components"
    )]
    pub(crate) fn finalize_member_output_paths(
        db: &crate::Database,
        event_tx: &broadcast::Sender<PipelineEvent>,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
        partial_path: &std::path::Path,
        out_path: &std::path::Path,
        chunk_dir: &std::path::Path,
    ) -> Result<u64, String> {
        Self::finalize_member_output(FinalizeMemberContext {
            db,
            event_tx,
            job_id,
            set_name,
            member_name,
            partial_path,
            out_path,
            chunk_dir,
        })
    }
}
