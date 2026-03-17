use super::*;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

/// Shared output file that multiple `DirectOutputWriter` instances write to.
/// Each writer tracks per-volume bytes but the underlying file is the same.
struct SharedOutputFile {
    inner: std::io::BufWriter<std::fs::File>,
}

/// Per-volume writer wrapper returned by the extraction writer factory.
/// Wraps a shared output file via `Rc<RefCell<...>>` and tracks bytes
/// written during this volume's contribution.
struct DirectOutputWriter {
    shared: Rc<RefCell<SharedOutputFile>>,
    bytes_written: u64,
}

impl std::io::Write for DirectOutputWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.shared.borrow_mut().inner.write(buf)?;
        self.bytes_written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.shared.borrow_mut().inner.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.shared.borrow_mut().inner.write_all(buf)?;
        self.bytes_written += buf.len() as u64;
        Ok(())
    }
}

impl Drop for DirectOutputWriter {
    fn drop(&mut self) {
        let _ = self.shared.borrow_mut().inner.flush();
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

        // Clean up any stale artifacts from previous extraction attempts.
        let chunk_dir = Self::member_chunk_dir(output_dir, set_name, &member_name);
        Self::clear_member_extraction_artifacts(
            db,
            job_id,
            set_name,
            &member_name,
            &partial_path,
            &chunk_dir,
        )?;

        // Open the shared output file — all volumes write directly here.
        let partial_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&partial_path)
            .map_err(|e| {
                format!(
                    "failed to create partial output {}: {e}",
                    partial_path.display()
                )
            })?;
        let shared = Rc::new(RefCell::new(SharedOutputFile {
            inner: std::io::BufWriter::with_capacity(8 * 1024 * 1024, partial_file),
        }));

        let chunk_records: Result<Vec<(u32, u64)>, weaver_rar::RarError> = if is_solid {
            let shared_ref = Rc::clone(&shared);
            archive
                .extract_member_solid_chunked(idx, options, |absolute_volume| {
                    let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                        weaver_rar::RarError::CorruptArchive {
                            detail: format!(
                                "solid chunk volume {absolute_volume} does not fit into u32"
                            ),
                        }
                    })?;
                    let _ = absolute_volume; // used only for the u32 check
                    Ok(Box::new(DirectOutputWriter {
                        shared: Rc::clone(&shared_ref),
                        bytes_written: 0,
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
            archive
                .extract_member_streaming_chunked(idx, options, &provider, |_local_volume| {
                    Ok(Box::new(DirectOutputWriter {
                        shared: Rc::clone(&shared_ref),
                        bytes_written: 0,
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

        // Flush the shared writer now that extraction is complete.
        if let Ok(mut shared_file) = shared.try_borrow_mut() {
            let _ = shared_file.inner.flush();
        }

        // Persist chunk offset records to DB for progress tracking.
        let partial_path_str = partial_path.to_string_lossy().to_string();
        let mut persisted_chunks = Vec::with_capacity(chunk_records.len());
        let mut next_offset = 0u64;
        for (absolute_volume, bytes_written) in &chunk_records {
            let start_offset = next_offset;
            let end_offset = start_offset + bytes_written;
            next_offset = end_offset;
            persisted_chunks.push(weaver_state::ExtractionChunk {
                member_name: member_name.clone(),
                volume_index: *absolute_volume,
                bytes_written: *bytes_written,
                temp_path: partial_path_str.clone(),
                start_offset,
                end_offset,
                verified: true,
                appended: true,
            });
        }
        if let Err(error) =
            db.replace_member_chunks(job_id, set_name, &member_name, &persisted_chunks)
        {
            let _ = std::fs::remove_file(&partial_path);
            return Err(format!("failed to persist extraction chunks: {error}"));
        }

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

    /// Try partial extraction: extract archive members whose volumes are all present.
    /// Called after each file completes, not just when all files are done.
    pub(super) async fn try_partial_extraction(&mut self, job_id: JobId) {
        // Check for ready 7z sets — extract each independently.
        let ready_sets: Vec<String> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .ready_archive_sets()
                .into_iter()
                .filter(|set_name| {
                    let dominated = state
                        .assembly
                        .archive_topology_for(set_name)
                        .is_some_and(|t| t.archive_type == weaver_assembly::ArchiveType::SevenZip);
                    dominated
                        && !self
                            .extracted_sets
                            .get(&job_id)
                            .is_some_and(|s| s.contains(set_name))
                })
                .collect()
        };

        for set_name in ready_sets {
            self.extracted_sets
                .entry(job_id)
                .or_default()
                .insert(set_name.clone());

            info!(job_id = job_id.0, set_name = %set_name, "spawning extraction for ready 7z set");
            let result = self.extract_7z_set(job_id, &set_name).await;
            if let Err(e) = result {
                warn!(job_id = job_id.0, set_name = %set_name, error = %e, "set extraction failed to start");
                if let Some(sets) = self.extracted_sets.get_mut(&job_id) {
                    sets.remove(&set_name);
                }
            }
        }

        self.try_batch_extraction(job_id).await;
    }

    /// Original batch extraction path: extract members whose volumes are all present.
    /// Scoped to a specific RAR set when multiple sets exist.
    async fn try_batch_extraction(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        match &state.status {
            JobStatus::Downloading
            | JobStatus::Verifying
            | JobStatus::Paused
            | JobStatus::Extracting => {}
            _ => return,
        }

        let active_workers = self
            .rar_sets
            .values()
            .map(|state| state.active_workers)
            .sum::<usize>();
        let available_slots = self
            .tuner
            .max_concurrent_extractions()
            .saturating_sub(active_workers);
        if available_slots == 0 {
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
                    self.try_partial_extraction(job_id).await;
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
                        // Set was pre-marked in extracted_sets; confirm it.
                        self.extracted_sets
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
                        // Don't mark set as extracted — needs repair + retry.
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
                    // Remove the pre-mark so it doesn't look extracted.
                    if let Some(sets) = self.extracted_sets.get_mut(&job_id) {
                        sets.remove(&set_name);
                    }
                    // Fail the job.
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state.status = JobStatus::Failed { error: e.clone() };
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::ExtractionFailed { job_id, error: e });
                        self.record_job_history(job_id);
                    }
                }
            },
        }
    }
}
