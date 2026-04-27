use super::checkpoint::{
    DirectOutputWriter, ExtractionCheckpointState, FinalizeMemberContext, SharedOutputFile,
};
use super::*;

pub(crate) struct RarExtractionContext<'a> {
    pub(crate) volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
    pub(crate) db: &'a crate::Database,
    pub(crate) event_tx: &'a broadcast::Sender<PipelineEvent>,
    pub(crate) job_id: JobId,
    pub(crate) set_name: &'a str,
    pub(crate) output_dir: &'a std::path::Path,
    pub(crate) options: &'a weaver_rar::ExtractOptions,
}

fn summarize_extraction_chunks(
    chunks: &[crate::ExtractionChunk],
) -> Vec<(u32, u64, u64, u64, bool, bool)> {
    chunks
        .iter()
        .map(|chunk| {
            (
                chunk.volume_index,
                chunk.bytes_written,
                chunk.start_offset,
                chunk.end_offset,
                chunk.verified,
                chunk.appended,
            )
        })
        .collect()
}

impl<'a> RarExtractionContext<'a> {
    pub(crate) fn new(
        volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
        db: &'a crate::Database,
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
    pub(crate) fn extract_rar_member_to_output(
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
        let existing_chunks: Vec<crate::ExtractionChunk> = db
            .get_extraction_chunks(job_id, set_name)
            .map_err(|e| format!("failed to load existing extraction chunks: {e}"))?
            .into_iter()
            .filter(|chunk| chunk.member_name == member_name)
            .collect();
        let existing_chunk_summary = summarize_extraction_chunks(&existing_chunks);
        let partial_size = std::fs::metadata(&partial_path).ok().map(|meta| meta.len());
        let out_size = std::fs::metadata(&out_path).ok().map(|meta| meta.len());
        info!(
            job_id = job_id.0,
            set_name,
            member = %member_name,
            idx,
            first_volume,
            last_volume,
            is_solid,
            available_volumes = ?volume_paths.keys().copied().collect::<Vec<_>>(),
            existing_chunk_rows = existing_chunks.len(),
            existing_chunks = ?existing_chunk_summary,
            partial_exists = partial_path.exists(),
            partial_size,
            out_exists = out_path.exists(),
            out_size,
            "RAR member extraction begin"
        );

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
                info!(
                    job_id = job_id.0,
                    set_name,
                    member = %member_name,
                    next_offset = checkpoint.next_offset,
                    completed_volumes = ?checkpoint.completed_volumes,
                    finalized_size,
                    unpacked_size,
                    "RAR member manifest already matches finalized output"
                );
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
                completed_volumes = ?checkpoint.completed_volumes,
                manifest = ?summarize_extraction_chunks(&checkpoint.manifest),
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
        let chunk_total = chunk_records
            .iter()
            .map(|(_, bytes_written)| *bytes_written)
            .sum::<u64>();
        let partial_size_after_extract =
            std::fs::metadata(&partial_path).ok().map(|meta| meta.len());
        info!(
            job_id = job_id.0,
            set_name,
            member = %member_name,
            chunk_records = ?chunk_records,
            chunk_total,
            unpacked_size,
            partial_size_after_extract,
            "RAR member extraction produced chunk records"
        );
        if chunk_total != unpacked_size {
            warn!(
                job_id = job_id.0,
                set_name,
                member = %member_name,
                chunk_total,
                unpacked_size,
                "RAR member chunk records do not sum to unpacked size"
            );
        }

        if let Some(error) = checkpoint.take_error() {
            let _ = std::fs::remove_file(&partial_path);
            return Err(error);
        }

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
        info!(
            job_id = job_id.0,
            set_name,
            member = %member_name,
            bytes_written,
            unpacked_size,
            out_path = %out_path.display(),
            "RAR member extraction finalized"
        );

        let _ = chunk_records;

        Ok((member_name, bytes_written, unpacked_size))
    }

    pub(crate) fn open_rar_archive_from_snapshot_or_disk(
        set_name: &str,
        volume_paths: std::collections::BTreeMap<u32, PathBuf>,
        password: Option<String>,
        cached_headers: Option<Vec<u8>>,
        refresh_provided_volumes: bool,
    ) -> Result<weaver_rar::RarArchive, String> {
        let has_cached_headers = cached_headers.is_some();
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
            let file = match std::fs::File::open(&path) {
                Ok(file) => file,
                Err(error)
                    if has_cached_headers && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "failed to open RAR volume {volume_number} for set '{set_name}': {error}"
                    ));
                }
            };
            if has_cached_headers
                && refresh_provided_volumes
                && archive.has_volume(volume_number as usize)
            {
                archive
                    .refresh_volume(volume_number as usize, Box::new(file))
                    .map_err(|e| {
                        format!(
                            "failed to refresh RAR volume {volume_number} for set '{set_name}': {e}"
                        )
                    })?;
            } else if archive.has_volume(volume_number as usize) {
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
}
