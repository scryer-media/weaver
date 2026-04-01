use super::*;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared output file that multiple `DirectOutputWriter` instances write to.
/// Each writer tracks per-volume bytes but the underlying file is the same.
struct SharedOutputFile {
    inner: std::io::BufWriter<std::fs::File>,
}

/// Per-volume writer wrapper returned by the extraction writer factory.
/// Wraps a shared output file via `Rc<RefCell<...>>` and tracks bytes
/// written during this volume's contribution.
struct DirectOutputWriter {
    shared: Option<Rc<RefCell<SharedOutputFile>>>,
    bytes_written: u64,
    volume_index: u32,
    checkpoint: Option<Arc<ExtractionCheckpointState>>,
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

struct ExtractionCheckpointState {
    db: weaver_state::Database,
    job_id: JobId,
    set_name: String,
    member_name: String,
    temp_path: String,
    manifest: Mutex<Vec<weaver_state::ExtractionChunk>>,
    next_offset: AtomicU64,
    error: Mutex<Option<String>>,
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
        manifest.push(weaver_state::ExtractionChunk {
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

    fn take_error(&self) -> Option<String> {
        self.error.lock().ok().and_then(|mut slot| slot.take())
    }
}

struct FinalizeMemberContext<'a> {
    db: &'a weaver_state::Database,
    event_tx: &'a broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &'a str,
    member_name: &'a str,
    partial_path: &'a std::path::Path,
    out_path: &'a std::path::Path,
    chunk_dir: &'a std::path::Path,
}

#[derive(Debug)]
struct ValidatedExtractionCheckpoint {
    manifest: Vec<weaver_state::ExtractionChunk>,
    completed_volumes: HashSet<u32>,
    next_offset: u64,
}

#[derive(Debug)]
struct ValidatedExtractionManifest {
    manifest: Vec<weaver_state::ExtractionChunk>,
    completed_volumes: HashSet<u32>,
    next_offset: u64,
}

pub(super) struct RarExtractionContext<'a> {
    volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
    db: &'a weaver_state::Database,
    event_tx: &'a broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &'a str,
    output_dir: &'a std::path::Path,
    options: &'a weaver_rar::ExtractOptions,
}

impl<'a> RarExtractionContext<'a> {
    pub(super) fn new(
        volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
        db: &'a weaver_state::Database,
        event_tx: &'a broadcast::Sender<PipelineEvent>,
        job_id: JobId,
        set_name: &'a str,
        output_dir: &'a std::path::Path,
        options: &'a weaver_rar::ExtractOptions,
    ) -> Self {
        Self {
            volume_paths,
            db,
            event_tx,
            job_id,
            set_name,
            output_dir,
            options,
        }
    }
}

impl Pipeline {
    fn is_recoverable_full_set_extraction_error(error: &str) -> bool {
        let lower = error.to_ascii_lowercase();
        lower.contains("checksum") || lower.contains("crc mismatch")
    }

    fn rar_set_worker_limit(plan: &crate::pipeline::rar_state::RarDerivedPlan) -> usize {
        if plan.is_solid { 1 } else { 2 }
    }

    fn extraction_chunk_root(output_dir: &std::path::Path) -> PathBuf {
        output_dir.join(".weaver-chunks")
    }

    fn member_output_paths(output_dir: &std::path::Path, member_name: &str) -> (PathBuf, PathBuf) {
        let out_path = output_dir.join(member_name);
        let partial_path = if let Some(ext) = out_path.extension() {
            out_path.with_extension(format!("{}.partial", ext.to_string_lossy()))
        } else {
            out_path.with_extension("partial")
        };
        (out_path, partial_path)
    }

    fn member_chunk_dir(
        output_dir: &std::path::Path,
        set_name: &str,
        member_name: &str,
    ) -> PathBuf {
        Self::extraction_chunk_root(output_dir)
            .join(super::job::sanitize_dirname(set_name))
            .join(member_name)
    }

    fn clear_member_extraction_artifacts(
        db: &weaver_state::Database,
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

    fn validate_member_extraction_manifest(
        chunks: &[weaver_state::ExtractionChunk],
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

    fn validate_member_extraction_checkpoint(
        chunks: &[weaver_state::ExtractionChunk],
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

    fn finalize_member_output(ctx: FinalizeMemberContext<'_>) -> Result<u64, String> {
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

        // Remove stale final output if it exists (e.g., from a previous attempt).
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

        // The partial file already contains the complete decompressed output
        // (written directly during extraction). Just rename to final path.
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

        // Clean up legacy chunk directory if it exists (backward compat
        // for interrupted upgrades from chunk-file extraction).
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

    pub(super) fn extract_rar_member_to_output(
        archive: &mut weaver_rar::RarArchive,
        ctx: RarExtractionContext<'_>,
        idx: usize,
    ) -> Result<(String, u64, u64), String> {
        let RarExtractionContext {
            volume_paths,
            db,
            event_tx,
            job_id,
            set_name,
            output_dir,
            options,
        } = ctx;
        let member = archive
            .member_info(idx)
            .ok_or_else(|| format!("member index {idx} missing from archive metadata"))?;
        let member_name = member.name.clone();
        let unpacked_size = member.unpacked_size.unwrap_or(0);
        let is_directory = member.is_directory;
        let first_volume = member.volumes.first_volume as u32;
        let last_volume = member.volumes.last_volume as u32;
        let is_solid = archive.is_solid();

        if is_directory {
            let dir_path = output_dir.join(&member_name);
            std::fs::create_dir_all(&dir_path)
                .map_err(|e| format!("failed to create dir {}: {e}", member_name))?;
            return Ok((member_name, 0, unpacked_size));
        }

        let (out_path, partial_path) = Self::member_output_paths(output_dir, &member_name);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create parent dir: {e}"))?;
        }

        let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
            job_id,
            set_name: set_name.to_string(),
            member: member_name.clone(),
        });
        crate::e2e_failpoint::maybe_delay("extract.member_start");

        let chunk_dir = Self::member_chunk_dir(output_dir, set_name, &member_name);
        let existing_chunks: Vec<weaver_state::ExtractionChunk> = db
            .get_extraction_chunks(job_id, set_name)
            .map_err(|e| format!("failed to load existing extraction chunks: {e}"))?
            .into_iter()
            .filter(|chunk| chunk.member_name == member_name)
            .collect();

        if out_path.exists()
            && let Ok(Some(checkpoint)) = Self::validate_member_extraction_manifest(
                &existing_chunks,
                first_volume,
                last_volume,
            )
        {
            let finalized_size = std::fs::metadata(&out_path)
                .map_err(|e| {
                    format!(
                        "failed to stat finalized output {}: {e}",
                        out_path.display()
                    )
                })?
                .len();
            if checkpoint.next_offset >= unpacked_size && finalized_size == unpacked_size {
                db.clear_member_chunks(job_id, set_name, &member_name)
                    .map_err(|e| format!("failed to clear extraction chunk rows: {e}"))?;
                match std::fs::remove_dir_all(&chunk_dir) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            path = %chunk_dir.display(),
                            error = %error,
                            "failed to remove legacy chunk dir during checkpoint reconcile"
                        );
                    }
                }
                return Ok((member_name, finalized_size, unpacked_size));
            }
        }

        let mut cleared_stale_artifacts = false;
        let resume_checkpoint = match Self::validate_member_extraction_checkpoint(
            &existing_chunks,
            &partial_path,
            first_volume,
            last_volume,
        ) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name,
                    member = %member_name,
                    error = %error,
                    "discarding invalid extraction checkpoint and restarting member extraction"
                );
                Self::clear_member_extraction_artifacts(
                    db,
                    job_id,
                    set_name,
                    &member_name,
                    &partial_path,
                    &chunk_dir,
                )?;
                cleared_stale_artifacts = true;
                None
            }
        };

        if let Some(checkpoint) = &resume_checkpoint {
            info!(
                job_id = job_id.0,
                set_name,
                member = %member_name,
                temp_path = %partial_path.display(),
                resumed_offset = checkpoint.next_offset,
                completed_volumes = checkpoint.completed_volumes.len(),
                "resuming extraction from persisted checkpoint"
            );
        }

        if resume_checkpoint.is_none()
            && !cleared_stale_artifacts
            && (!existing_chunks.is_empty() || partial_path.exists())
        {
            Self::clear_member_extraction_artifacts(
                db,
                job_id,
                set_name,
                &member_name,
                &partial_path,
                &chunk_dir,
            )?;
        }

        // Open the shared output file — all volumes write directly here.
        let mut partial_file_options = std::fs::OpenOptions::new();
        partial_file_options.create(true).write(true);
        if resume_checkpoint.is_some() {
            partial_file_options.append(true);
        } else {
            partial_file_options.truncate(true);
        }
        let partial_file = partial_file_options.open(&partial_path).map_err(|e| {
            format!(
                "failed to create partial output {}: {e}",
                partial_path.display()
            )
        })?;
        let shared = Rc::new(RefCell::new(SharedOutputFile {
            inner: std::io::BufWriter::with_capacity(8 * 1024 * 1024, partial_file),
        }));
        let resumed_manifest = resume_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.manifest.clone())
            .unwrap_or_default();
        let resumed_offset = resume_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.next_offset)
            .unwrap_or(0);
        let completed_volumes = resume_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.completed_volumes.clone())
            .unwrap_or_default();
        let checkpoint = Arc::new(ExtractionCheckpointState {
            db: db.clone(),
            job_id,
            set_name: set_name.to_string(),
            member_name: member_name.clone(),
            temp_path: partial_path.to_string_lossy().to_string(),
            manifest: Mutex::new(resumed_manifest),
            next_offset: AtomicU64::new(resumed_offset),
            error: Mutex::new(None),
        });

        let chunk_records: Result<Vec<(u32, u64)>, weaver_rar::RarError> = if is_solid {
            let shared_ref = Rc::clone(&shared);
            let checkpoint_ref = Arc::clone(&checkpoint);
            let completed_volumes = completed_volumes.clone();
            archive
                .extract_member_solid_chunked(idx, options, |absolute_volume| {
                    let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                        weaver_rar::RarError::CorruptArchive {
                            detail: format!(
                                "solid chunk volume {absolute_volume} does not fit into u32"
                            ),
                        }
                    })?;
                    let already_completed = completed_volumes.contains(&absolute_volume);
                    Ok(Box::new(DirectOutputWriter {
                        shared: if already_completed {
                            None
                        } else {
                            Some(Rc::clone(&shared_ref))
                        },
                        bytes_written: 0,
                        volume_index: absolute_volume,
                        checkpoint: if already_completed {
                            None
                        } else {
                            Some(Arc::clone(&checkpoint_ref))
                        },
                    }) as Box<dyn Write>)
                })
                .and_then(|records| {
                    records
                        .into_iter()
                        .map(|(absolute_volume, bytes_written)| {
                            let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                                weaver_rar::RarError::CorruptArchive {
                                    detail: format!(
                                        "solid chunk volume {absolute_volume} does not fit into u32"
                                    ),
                                }
                            })?;
                            Ok((absolute_volume, bytes_written))
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
        } else {
            let mut provider_paths = std::collections::HashMap::new();
            for absolute_volume in first_volume..=last_volume {
                let Some(path) = volume_paths.get(&absolute_volume) else {
                    return Err(format!(
                        "missing local RAR volume {absolute_volume} for member {member_name}"
                    ));
                };
                provider_paths.insert((absolute_volume - first_volume) as usize, path.clone());
            }
            let provider = weaver_rar::StaticVolumeProvider::new(provider_paths);
            let shared_ref = Rc::clone(&shared);
            let checkpoint_ref = Arc::clone(&checkpoint);
            let completed_volumes = completed_volumes.clone();
            archive
                .extract_member_streaming_chunked(idx, options, &provider, |local_volume| {
                    let volume_index = first_volume + local_volume as u32;
                    let already_completed = completed_volumes.contains(&volume_index);
                    Ok(Box::new(DirectOutputWriter {
                        shared: if already_completed {
                            None
                        } else {
                            Some(Rc::clone(&shared_ref))
                        },
                        bytes_written: 0,
                        volume_index,
                        checkpoint: if already_completed {
                            None
                        } else {
                            Some(Arc::clone(&checkpoint_ref))
                        },
                    }) as Box<dyn Write>)
                })
                .and_then(|records| {
                    records
                        .into_iter()
                        .map(|(local_volume, bytes_written)| {
                            Ok((first_volume + local_volume as u32, bytes_written))
                        })
                        .collect::<Result<Vec<_>, weaver_rar::RarError>>()
                })
        };
        let chunk_records = chunk_records.map_err(|error| {
            let _ = std::fs::remove_file(&partial_path);
            format!("failed to extract {member_name}: {error}")
        })?;
        if let Some(error) = checkpoint.take_error() {
            let _ = std::fs::remove_file(&partial_path);
            return Err(error);
        }

        // Flush, sync to NFS, and close the file handle before finalizing.
        // Without this, the rename() in finalize forces the NFS client to
        // flush all dirty pages (potentially 60+ GB) synchronously, causing
        // a multi-minute stall attributed to the "append" phase.
        {
            let mut shared_file = shared.borrow_mut();
            shared_file.inner.flush().map_err(|e| {
                format!(
                    "failed to flush partial output {}: {e}",
                    partial_path.display()
                )
            })?;
            shared_file.inner.get_ref().sync_all().map_err(|e| {
                format!(
                    "failed to sync partial output {}: {e}",
                    partial_path.display()
                )
            })?;
        }
        drop(shared);

        // Finalize: rename .partial → final output, emit events, clear DB rows.
        let bytes_written = match Self::finalize_member_output(FinalizeMemberContext {
            db,
            event_tx,
            job_id,
            set_name,
            member_name: &member_name,
            partial_path: &partial_path,
            out_path: &out_path,
            chunk_dir: &chunk_dir,
        }) {
            Ok(bytes_written) => bytes_written,
            Err(error) => {
                let _ = std::fs::remove_file(&partial_path);
                return Err(error);
            }
        };

        let _ = chunk_records;

        Ok((member_name, bytes_written, unpacked_size))
    }

    pub(super) fn open_rar_archive_from_snapshot_or_disk(
        set_name: &str,
        volume_paths: std::collections::BTreeMap<u32, PathBuf>,
        password: Option<String>,
        cached_headers: Option<Vec<u8>>,
    ) -> Result<weaver_rar::RarArchive, String> {
        let mut archive = match cached_headers {
            Some(headers) => weaver_rar::RarArchive::deserialize_headers_with_password(
                &headers,
                password.clone(),
            )
            .map_err(|e| {
                format!("failed to deserialize cached RAR headers for set '{set_name}': {e}")
            })?,
            None => {
                let first_path = volume_paths.get(&0).ok_or_else(|| {
                    format!("RAR set '{set_name}' cannot be opened without volume 0")
                })?;
                let first_file = std::fs::File::open(first_path)
                    .map_err(|e| format!("failed to open RAR volume 0: {e}"))?;
                if let Some(password) = password.as_deref() {
                    weaver_rar::RarArchive::open_with_password(first_file, password)
                } else {
                    weaver_rar::RarArchive::open(first_file)
                }
                .map_err(|e| format!("failed to parse RAR volume 0 for set '{set_name}': {e}"))?
            }
        };

        for (volume_number, path) in volume_paths {
            let file = std::fs::File::open(&path).map_err(|e| {
                format!("failed to open RAR volume {volume_number} for set '{set_name}': {e}")
            })?;
            if archive.has_volume(volume_number as usize) {
                archive.attach_volume_reader(volume_number as usize, Box::new(file));
            } else {
                archive
                    .add_volume(volume_number as usize, Box::new(file))
                    .map_err(|e| {
                        format!(
                            "failed to integrate RAR volume {volume_number} for set '{set_name}': {e}"
                        )
                    })?;
            }
        }

        Ok(archive)
    }

    /// Try incremental RAR extraction: extract members whose volumes are all present.
    /// Called after each file completes, not just when all files are done.
    /// Non-RAR archives (7z, zip, tar) are handled in check_job_completion via
    /// ExtractionReadiness::Partial/Ready — they are all-or-nothing, not incremental.
    pub(super) async fn try_rar_extraction(&mut self, job_id: JobId) {
        self.try_batch_extraction(job_id).await;
    }

    /// Original batch extraction path: extract members whose volumes are all present.
    /// Scoped to a specific RAR set when multiple sets exist.
    async fn try_batch_extraction(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        match &state.status {
            JobStatus::Downloading | JobStatus::Verifying | JobStatus::Extracting => {}
            _ => return,
        }

        let active_workers = self
            .rar_sets
            .values()
            .map(|state| state.active_workers)
            .sum::<usize>();
        let job_active_workers = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .map(|(_, state)| state.active_workers)
            .sum::<usize>();
        let available_slots = self
            .tuner
            .max_concurrent_extractions()
            .saturating_sub(active_workers);
        if available_slots == 0 {
            if job_active_workers == 0
                && self
                    .jobs
                    .get(&job_id)
                    .is_some_and(|state| matches!(state.status, JobStatus::Extracting))
            {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::QueuedExtract,
                    Some("queued_extract"),
                );
            }
            return;
        }

        let mut ready_sets: Vec<(String, Vec<String>)> = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .filter_map(|((_, set_name), set_state)| {
                let plan = set_state.plan.as_ref()?;
                let worker_limit = Self::rar_set_worker_limit(plan);
                let free_workers = worker_limit.saturating_sub(set_state.active_workers);
                if free_workers == 0 || plan.ready_members.is_empty() {
                    return None;
                }
                let mut seen_members = HashSet::new();
                let mut members = Vec::new();
                for ready_member in &plan.ready_members {
                    if !seen_members.insert(ready_member.name.clone()) {
                        continue;
                    }
                    if set_state.in_flight_members.contains(&ready_member.name) {
                        continue;
                    }
                    members.push(ready_member.name.clone());
                    if members.len() >= free_workers {
                        break;
                    }
                }
                (!members.is_empty()).then_some((set_name.clone(), members))
            })
            .collect();
        ready_sets.sort_by(|a, b| a.0.cmp(&b.0));
        if ready_sets.is_empty() {
            return;
        }
        if !self.maybe_start_extraction(job_id).await {
            return;
        }

        let mut scheduled_slots = 0usize;
        for (set_name, ready_members) in ready_sets {
            for member_name in ready_members {
                if scheduled_slots >= available_slots {
                    return;
                }

                let members_to_extract = vec![member_name.clone()];
                let volume_paths_map = self.volume_paths_for_rar_set(job_id, &set_name);
                if volume_paths_map.is_empty() {
                    continue;
                }
                let cached_headers = self.load_rar_snapshot(job_id, &set_name);

                let password = self.jobs.get(&job_id).unwrap().spec.password.clone();

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    known_volumes = volume_paths_map.len(),
                    cached_headers = cached_headers.is_some(),
                    "RAR incremental extraction member ready"
                );

                if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                    set_state.active_workers += 1;
                    set_state.in_flight_members.insert(member_name.clone());
                    set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
                    if let Some(plan) = set_state.plan.as_mut() {
                        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
                    }
                }
                scheduled_slots += 1;

                let output_dir = self.extraction_staging_dir(job_id);
                let event_tx = self.event_tx.clone();
                let db = self.db.clone();
                let attempted = members_to_extract.clone();
                let extract_done_tx = self.extract_done_tx.clone();
                let set_name_owned = set_name.clone();
                let set_name_for_task = set_name.clone();
                let set_name_for_archive = set_name.clone();
                let pp_pool = self.pp_pool.clone();
                tokio::task::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        pp_pool.install(move || {
                            let mut archive = Self::open_rar_archive_from_snapshot_or_disk(
                                &set_name_for_archive,
                                volume_paths_map.clone(),
                                password.clone(),
                                cached_headers,
                            )?;

                            let options = weaver_rar::ExtractOptions {
                                verify: true,
                                password: password.clone(),
                            };
                            let mut outcome = BatchExtractionOutcome {
                                extracted: Vec::new(),
                                failed: Vec::new(),
                            };

                            for member_name in &members_to_extract {
                                let Some(idx) = archive.find_member_sanitized(member_name) else {
                                    outcome.failed.push((
                                        member_name.clone(),
                                        "member not found in archive".to_string(),
                                    ));
                                    continue;
                                };

                                match Self::extract_rar_member_to_output(
                                    &mut archive,
                                    RarExtractionContext {
                                        volume_paths: &volume_paths_map,
                                        db: &db,
                                        event_tx: &event_tx,
                                        job_id,
                                        set_name: &set_name_for_task,
                                        output_dir: &output_dir,
                                        options: &options,
                                    },
                                    idx,
                                ) {
                                    Ok((extracted_name, bytes_written, total_bytes)) => {
                                        let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                                            job_id,
                                            member: extracted_name.clone(),
                                            bytes_written,
                                            total_bytes,
                                        });
                                        let _ = event_tx.send(
                                            PipelineEvent::ExtractionMemberFinished {
                                                job_id,
                                                set_name: set_name_for_task.clone(),
                                                member: extracted_name.clone(),
                                            },
                                        );
                                        outcome.extracted.push(extracted_name);
                                    }
                                    Err(error) => {
                                        let _ =
                                            event_tx.send(PipelineEvent::ExtractionMemberFailed {
                                                job_id,
                                                set_name: set_name_for_task.clone(),
                                                member: member_name.clone(),
                                                error: error.clone(),
                                            });
                                        outcome.failed.push((member_name.clone(), error));
                                    }
                                }
                            }

                            Ok(outcome)
                        })
                    })
                    .await;

                    let result = match result {
                        Ok(result) => result,
                        Err(error) => Err(format!("extraction task panicked: {error}")),
                    };
                    let _ = extract_done_tx
                        .send(ExtractionDone::Batch {
                            job_id,
                            set_name: set_name_owned,
                            attempted,
                            result,
                        })
                        .await;
                });
            }
        }
    }

    fn member_volume_span(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Option<(u32, u32)> {
        let state = self.jobs.get(&job_id)?;
        let topo = state.assembly.archive_topology_for(set_name)?;
        let member = topo.members.iter().find(|m| m.name == member_name)?;
        Some((member.first_volume, member.last_volume))
    }

    pub(super) fn clear_failed_extraction_member(&mut self, job_id: JobId, member_name: &str) {
        let mut remove_entry = false;
        if let Some(failed) = self.failed_extractions.get_mut(&job_id) {
            failed.remove(member_name);
            remove_entry = failed.is_empty();
        }
        if remove_entry {
            self.failed_extractions.remove(&job_id);
        }
        if let Err(error) = self.db.remove_failed_extraction(job_id, member_name) {
            error!(
                job_id = job_id.0,
                member = %member_name,
                error = %error,
                "failed to clear persisted failed extraction member"
            );
        }
    }

    fn suspect_par2_file_ids_for_member(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Vec<weaver_par2::FileId> {
        let Some(par2_set) = self.par2_set(job_id) else {
            return Vec::new();
        };
        let Some((first_volume, last_volume)) =
            self.member_volume_span(job_id, set_name, member_name)
        else {
            return Vec::new();
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let Some(topo) = state.assembly.archive_topology_for(set_name) else {
            return Vec::new();
        };

        let filename_to_file_id: HashMap<&str, weaver_par2::FileId> = par2_set
            .recovery_file_ids
            .iter()
            .filter_map(|file_id| {
                par2_set
                    .file_description(file_id)
                    .map(|desc| (desc.filename.as_str(), *file_id))
            })
            .collect();

        let mut file_ids = Vec::new();
        let target_first = first_volume.saturating_sub(1);
        let target_last = last_volume.saturating_add(1);
        for (filename, &volume_number) in &topo.volume_map {
            if (target_first..=target_last).contains(&volume_number)
                && let Some(file_id) = filename_to_file_id.get(filename.as_str())
            {
                file_ids.push(*file_id);
            }
        }
        file_ids.sort_unstable_by_key(|id| *id.as_bytes());
        file_ids.dedup();
        file_ids
    }

    async fn promote_recovery_for_failed_member(
        &mut self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) {
        let file_ids = self.suspect_par2_file_ids_for_member(job_id, set_name, member_name);
        if file_ids.is_empty() {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                member = %member_name,
                "no PAR2 file ids available for lower-bound verification"
            );
            return;
        }

        let (working_dir, par2_set) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(par2_set) = self.par2_set(job_id).cloned() else {
                return;
            };
            (state.working_dir.clone(), par2_set)
        };

        let pp_pool = self.pp_pool.clone();
        let lower_bound = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || -> Result<u32, String> {
                let plan = weaver_par2::scan_placement(&working_dir, &par2_set)
                    .map_err(|e| format!("placement scan failed: {e}"))?;
                let selected: HashSet<weaver_par2::FileId> = file_ids.iter().copied().collect();
                if plan
                    .conflicts
                    .iter()
                    .any(|file_id| selected.contains(file_id))
                {
                    return Err("placement conflicts in suspect files".to_string());
                }

                let access =
                    weaver_par2::PlacementFileAccess::from_plan(working_dir, &par2_set, &plan);
                let verification =
                    weaver_par2::verify_selected_file_ids(&par2_set, &access, &file_ids);
                Ok(verification.total_missing_blocks)
            })
        })
        .await;

        match lower_bound {
            Ok(Ok(blocks_needed)) if blocks_needed > 0 => {
                let promoted = self.promote_recovery_targeted(job_id, blocks_needed);
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    blocks_needed,
                    promoted_blocks = promoted,
                    "targeted recovery promotion from lower-bound verify"
                );
            }
            Ok(Ok(_)) => {
                debug!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    "lower-bound verify found no missing slices"
                );
            }
            Ok(Err(error)) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    error = %error,
                    "skipping lower-bound targeted promotion"
                );
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    member = %member_name,
                    error = %error,
                    "lower-bound verification task panicked"
                );
            }
        }
    }

    /// Handle a completed background extraction task.
    pub(super) async fn handle_extraction_done(&mut self, done: ExtractionDone) {
        match done {
            ExtractionDone::Batch {
                job_id,
                set_name,
                attempted,
                result,
            } => {
                if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                    set_state.active_workers = set_state.active_workers.saturating_sub(1);
                    for member in &attempted {
                        set_state.in_flight_members.remove(member);
                    }
                }

                match result {
                    Ok(outcome) => {
                        for name in &outcome.extracted {
                            info!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                member = %name,
                                "RAR batch member extracted"
                            );
                            self.extracted_members
                                .entry(job_id)
                                .or_default()
                                .insert(name.clone());
                            if let Some(state) = self.jobs.get(&job_id) {
                                let output_path = state.working_dir.join(name);
                                if let Err(error) =
                                    self.db.add_extracted_member(job_id, name, &output_path)
                                {
                                    error!(
                                        job_id = job_id.0,
                                        set_name = %set_name,
                                        member = %name,
                                        error = %error,
                                        "failed to persist extracted member"
                                    );
                                }
                            }
                            self.clear_failed_extraction_member(job_id, name);
                        }
                        for (member, error) in &outcome.failed {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                member = %member,
                                error = %error,
                                "RAR batch member failed"
                            );
                            self.set_failed_extraction_member(job_id, member);
                            self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                .await;
                        }
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            error = %error,
                            "RAR batch extraction worker failed"
                        );
                        for member in &attempted {
                            self.set_failed_extraction_member(job_id, member);
                            self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                .await;
                        }
                    }
                }

                if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed to refresh RAR set state after batch extraction"
                    );
                }
                self.try_delete_volumes(job_id, &set_name);
                let all_downloaded = self.jobs.get(&job_id).is_some_and(|state| {
                    state.assembly.complete_data_file_count() >= state.assembly.data_file_count()
                });
                if all_downloaded {
                    self.check_job_completion(job_id).await;
                } else {
                    self.try_rar_extraction(job_id).await;
                    if !self.has_active_rar_workers(job_id)
                        && self
                            .jobs
                            .get(&job_id)
                            .is_some_and(|state| matches!(state.status, JobStatus::Extracting))
                    {
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                    }
                }
            }
            ExtractionDone::FullSet {
                job_id,
                set_name,
                result,
            } => match result {
                Ok(outcome) => {
                    if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                        set_state.active_workers = 0;
                        set_state.in_flight_members.clear();
                    }
                    for member in &outcome.extracted {
                        self.extracted_members
                            .entry(job_id)
                            .or_default()
                            .insert(member.clone());
                        if let Some(state) = self.jobs.get(&job_id) {
                            let output_path = state.working_dir.join(member);
                            let _ = self.db.add_extracted_member(job_id, member, &output_path);
                        }
                        self.clear_failed_extraction_member(job_id, member);
                    }

                    if outcome.failed.is_empty() {
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            members = outcome.extracted.len(),
                            "set extraction complete"
                        );
                        // Promote from spawned to extracted now that the task is done.
                        if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                            sets.remove(&set_name);
                        }
                        self.extracted_archives
                            .entry(job_id)
                            .or_default()
                            .insert(set_name.clone());
                    } else {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            succeeded = outcome.extracted.len(),
                            failed = ?outcome.failed,
                            "set extraction completed with failures"
                        );
                        // Track failed members so they can be retried after repair.
                        for member in &outcome.failed {
                            if let Some(members) = self.extracted_members.get_mut(&job_id) {
                                members.remove(member);
                            }
                            self.set_failed_extraction_member(job_id, member);
                            self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                .await;
                        }
                        // Remove from inflight so the job doesn't deadlock waiting
                        // for this set to complete. check_job_completion will either
                        // trigger PAR2 repair or fail the job.
                        if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                            sets.remove(&set_name);
                        }
                    }
                    if self.rar_sets.contains_key(&(job_id, set_name.clone())) {
                        let _ = self.recompute_rar_set_state(job_id, &set_name).await;
                    }
                    self.try_delete_volumes(job_id, &set_name);
                    self.check_job_completion(job_id).await;
                }
                Err(e) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %e,
                        "set extraction failed"
                    );
                    if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                        set_state.active_workers = 0;
                        set_state.in_flight_members.clear();
                    }
                    // Remove from spawned tracking so it doesn't block retries.
                    if let Some(sets) = self.inflight_extractions.get_mut(&job_id) {
                        sets.remove(&set_name);
                    }
                    if Self::is_recoverable_full_set_extraction_error(&e) {
                        self.set_failed_extraction_member(job_id, &set_name);
                        self.check_job_completion(job_id).await;
                        return;
                    }
                    let _ = self.event_tx.send(PipelineEvent::ExtractionFailed {
                        job_id,
                        error: e.clone(),
                    });
                    self.fail_job(job_id, e);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest(temp_path: &std::path::Path) -> Vec<weaver_state::ExtractionChunk> {
        vec![
            weaver_state::ExtractionChunk {
                member_name: "movie.mkv".to_string(),
                volume_index: 0,
                bytes_written: 10,
                temp_path: temp_path.to_string_lossy().to_string(),
                start_offset: 0,
                end_offset: 10,
                verified: true,
                appended: false,
            },
            weaver_state::ExtractionChunk {
                member_name: "movie.mkv".to_string(),
                volume_index: 1,
                bytes_written: 15,
                temp_path: temp_path.to_string_lossy().to_string(),
                start_offset: 10,
                end_offset: 25,
                verified: true,
                appended: false,
            },
        ]
    }

    #[test]
    fn validate_member_extraction_checkpoint_accepts_contiguous_manifest() {
        let temp_dir = tempfile::tempdir().unwrap();
        let partial_path = temp_dir.path().join("movie.mkv.partial");
        std::fs::write(&partial_path, vec![0u8; 25]).unwrap();

        let checkpoint = Pipeline::validate_member_extraction_checkpoint(
            &sample_manifest(&partial_path),
            &partial_path,
            0,
            2,
        )
        .unwrap()
        .unwrap();

        assert_eq!(checkpoint.next_offset, 25);
        assert_eq!(checkpoint.completed_volumes, HashSet::from([0u32, 1u32]));
    }

    #[test]
    fn validate_member_extraction_checkpoint_rejects_partial_size_mismatch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let partial_path = temp_dir.path().join("movie.mkv.partial");
        std::fs::write(&partial_path, vec![0u8; 20]).unwrap();

        let error = Pipeline::validate_member_extraction_checkpoint(
            &sample_manifest(&partial_path),
            &partial_path,
            0,
            2,
        )
        .unwrap_err();

        assert!(error.contains("manifest ends at 25"));
    }

    #[test]
    fn validate_member_extraction_manifest_allows_finalize_reconcile_without_partial_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let partial_path = temp_dir.path().join("movie.mkv.partial");

        let manifest =
            Pipeline::validate_member_extraction_manifest(&sample_manifest(&partial_path), 0, 2)
                .unwrap()
                .unwrap();

        assert_eq!(manifest.next_offset, 25);
        assert_eq!(manifest.completed_volumes, HashSet::from([0u32, 1u32]));
    }
}
