use super::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

fn extract_zip(
    archive_path: &Path,
    output_dir: &Path,
    password: Option<&str>,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open zip: {e}"))?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| format!("failed to read zip archive: {e}"))?;
    let mut extracted = Vec::new();

    for i in 0..archive.len() {
        let mut entry = if let Some(pw) = password {
            archive
                .by_index_decrypt(i, pw.as_bytes())
                .map_err(|e| format!("failed to read zip entry {i}: {e}"))?
        } else {
            archive
                .by_index(i)
                .map_err(|e| format!("failed to read zip entry {i}: {e}"))?
        };
        let name = entry.name().to_string();

        if entry.is_dir() {
            let dir_path = output_dir.join(&name);
            std::fs::create_dir_all(&dir_path)
                .map_err(|e| format!("failed to create dir {name}: {e}"))?;
            continue;
        }

        let out_path = output_dir.join(&name);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create parent dir: {e}"))?;
        }
        let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });

        let mut outfile = std::fs::File::create(&out_path)
            .map_err(|e| format!("failed to create {name}: {e}"))?;
        let bytes_written = std::io::copy(&mut entry, &mut outfile)
            .map_err(|e| format!("failed to extract {name}: {e}"))?;

        let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });
        tracing::info!(job_id = job_id.0, member = %name, bytes_written, "zip member extracted");
        extracted.push(name);
    }

    Ok(extracted)
}

fn extract_tar(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open tar: {e}"))?;
    extract_tar_from_reader(file, output_dir, event_tx, job_id, set_name)
}

fn extract_tar_gz(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open tar.gz: {e}"))?;
    let gz = flate2::read::GzDecoder::new(file);
    extract_tar_from_reader(gz, output_dir, event_tx, job_id, set_name)
}

fn extract_tar_bz2(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open tar.bz2: {e}"))?;
    let bz2 = bzip2::read::BzDecoder::new(file);
    extract_tar_from_reader(bz2, output_dir, event_tx, job_id, set_name)
}

fn extract_tar_from_reader<R: std::io::Read>(
    reader: R,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let mut archive = tar::Archive::new(reader);
    let mut extracted = Vec::new();

    for entry in archive
        .entries()
        .map_err(|e| format!("failed to read tar entries: {e}"))?
    {
        let mut entry = entry.map_err(|e| format!("failed to read tar entry: {e}"))?;
        let path = entry
            .path()
            .map_err(|e| format!("invalid tar entry path: {e}"))?
            .to_path_buf();
        let name = path.to_string_lossy().to_string();

        let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });

        entry
            .unpack_in(output_dir)
            .map_err(|e| format!("failed to extract tar entry {name}: {e}"))?;

        let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
            job_id,
            set_name: set_name.to_string(),
            member: name.clone(),
        });
        tracing::info!(job_id = job_id.0, member = %name, "tar member extracted");
        extracted.push(name);
    }

    Ok(extracted)
}

fn extract_gz(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open gz: {e}"))?;
    let mut gz = flate2::read::GzDecoder::new(file);

    // Output filename: strip .gz extension
    let archive_name = archive_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let output_name = archive_name
        .strip_suffix(".gz")
        .or_else(|| archive_name.strip_suffix(".GZ"))
        .unwrap_or(&archive_name);
    let out_path = output_dir.join(output_name);

    let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });

    let mut outfile = std::fs::File::create(&out_path)
        .map_err(|e| format!("failed to create {output_name}: {e}"))?;
    let bytes_written = std::io::copy(&mut gz, &mut outfile)
        .map_err(|e| format!("failed to decompress gz: {e}"))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });
    tracing::info!(job_id = job_id.0, member = %output_name, bytes_written, "gz decompressed");

    Ok(vec![output_name.to_string()])
}

fn strip_ascii_case_suffix<'a>(name: &'a str, suffix: &str) -> Option<&'a str> {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with(suffix) {
        Some(&name[..name.len() - suffix.len()])
    } else {
        None
    }
}

fn derive_single_file_output_name<'a>(archive_name: &'a str, suffixes: &[&str]) -> &'a str {
    suffixes
        .iter()
        .find_map(|suffix| strip_ascii_case_suffix(archive_name, suffix))
        .unwrap_or(archive_name)
}

#[allow(clippy::too_many_arguments)]
fn extract_single_stream_to_file<R: std::io::Read>(
    mut reader: R,
    archive_path: &Path,
    output_dir: &Path,
    suffixes: &[&str],
    format_name: &str,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let archive_name = archive_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let output_name = derive_single_file_output_name(&archive_name, suffixes);
    let out_path = output_dir.join(output_name);

    let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });

    let mut outfile = std::fs::File::create(&out_path)
        .map_err(|e| format!("failed to create {output_name}: {e}"))?;
    let bytes_written = std::io::copy(&mut reader, &mut outfile)
        .map_err(|e| format!("failed to decompress {format_name}: {e}"))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });
    tracing::info!(job_id = job_id.0, member = %output_name, bytes_written, format = format_name, "compressed file decompressed");

    Ok(vec![output_name.to_string()])
}

fn extract_brotli(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open br: {e}"))?;
    let reader = brotli::Decompressor::new(file, 4096);
    extract_single_stream_to_file(
        reader,
        archive_path,
        output_dir,
        &[".br"],
        "br",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_deflate(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open deflate: {e}"))?;
    let reader = flate2::read::DeflateDecoder::new(file);
    extract_single_stream_to_file(
        reader,
        archive_path,
        output_dir,
        &[".deflate"],
        "deflate",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_zstd(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("failed to open zstd: {e}"))?;
    let reader =
        zstd::stream::read::Decoder::new(file).map_err(|e| format!("failed to open zstd: {e}"))?;
    extract_single_stream_to_file(
        reader,
        archive_path,
        output_dir,
        &[".zstd", ".zst"],
        "zstd",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_bzip2(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open bz2: {e}"))?;
    let reader = bzip2::read::BzDecoder::new(file);
    extract_single_stream_to_file(
        reader,
        archive_path,
        output_dir,
        &[".bz2"],
        "bz2",
        event_tx,
        job_id,
        set_name,
    )
}

fn extract_split(
    file_paths: &[PathBuf],
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    // Output filename: the base name from the set (e.g., "movie.mkv" from "movie.mkv.001")
    let first_name = file_paths[0]
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let output_name = if let Some(dot_pos) = first_name.rfind('.') {
        &first_name[..dot_pos]
    } else {
        &first_name
    };
    let out_path = output_dir.join(output_name);

    let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });

    let mut reader = crate::pipeline::archive::split_reader::SplitFileReader::open(file_paths)
        .map_err(|e| format!("failed to open split files: {e}"))?;
    let mut outfile = std::fs::File::create(&out_path)
        .map_err(|e| format!("failed to create {output_name}: {e}"))?;
    let bytes_written = std::io::copy(&mut reader, &mut outfile)
        .map_err(|e| format!("failed to concatenate split files: {e}"))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
        job_id,
        set_name: set_name.to_string(),
        member: output_name.to_string(),
    });
    tracing::info!(job_id = job_id.0, member = %output_name, bytes_written, parts = file_paths.len(), "split files joined");

    Ok(vec![output_name.to_string()])
}

impl Pipeline {
    pub(crate) async fn extract_rar_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let (volume_paths, cached_headers, password) = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            (
                self.volume_paths_for_rar_set(job_id, set_name),
                self.load_rar_snapshot(job_id, set_name),
                state.spec.password.clone(),
            )
        };

        if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.to_string())) {
            set_state.active_workers = 1;
            set_state.in_flight_members.clear();
            set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
            if let Some(plan) = set_state.plan.as_mut() {
                plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
            }
        }

        // Collect already-extracted members so we skip them.
        let already_extracted: HashSet<String> = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();

        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_owned = set_name.to_string();
        let set_name_for_task = set_name.to_string();
        let event_tx = self.event_tx.clone();
        let db = self.db.clone();
        let output_dir = self.extraction_staging_dir(job_id);
        let set_name_for_result = set_name_owned.clone();
        let pp_pool = self.pp_pool.clone();
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || pp_pool.install(move || {
                if volume_paths.is_empty() {
                    return Err(format!("no on-disk RAR volumes for set '{set_name_owned}'"));
                }

                let mut archive = Self::open_rar_archive_from_snapshot_or_disk(
                    &set_name_owned,
                    volume_paths.clone(),
                    password.clone(),
                    cached_headers,
                )?;

                let meta = archive.metadata();
                let options = weaver_rar::ExtractOptions {
                    verify: true,
                    password: password.clone(),
                };
                let is_solid = archive.is_solid();

                let mut extracted_members = Vec::new();
                let mut failed_members: Vec<String> = Vec::new();
                for (idx, member) in meta.members.iter().enumerate() {
                    if already_extracted.contains(&member.name) {
                        continue;
                    }

                    match Self::extract_rar_member_to_output(
                        &mut archive,
                        crate::pipeline::extraction::RarExtractionContext::new(
                            &volume_paths,
                            &db,
                            &event_tx,
                            job_id,
                            &set_name_for_task,
                            &output_dir,
                            &options,
                        ),
                        idx,
                    ) {
                        Ok((member_name, bytes_written, total_bytes)) => {
                            info!(job_id = job_id.0, member = %member_name, bytes_written, total_bytes, "member extracted");
                            let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                                job_id,
                                member: member_name.clone(),
                                bytes_written,
                                total_bytes,
                            });
                            let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
                                job_id,
                                set_name: set_name_for_task.clone(),
                                member: member_name.clone(),
                            });
                            extracted_members.push(member_name);
                        }
                        Err(e) => {
                            let _ = event_tx.send(PipelineEvent::ExtractionMemberFailed {
                                job_id,
                                set_name: set_name_for_task.clone(),
                                member: member.name.clone(),
                                error: e.to_string(),
                            });
                            tracing::warn!(member = %member.name, error = %e, "member extraction failed, continuing with remaining members");
                            failed_members.push(member.name.clone());
                            if is_solid {
                                break;
                            }
                        }
                    }
                }

                Ok(FullSetExtractionOutcome {
                    extracted: extracted_members,
                    failed: failed_members,
                })
            }))
            .await;

            let result = match result {
                Ok(result) => result,
                Err(e) => Err(format!("extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_for_result,
                    result,
                })
                .await;
        });

        // Extraction runs in background — result comes through extract_done_tx channel.
        Ok(0)
    }

    /// Spawn extraction for a list of archives, tracking each in `inflight_extractions`.
    /// Dispatches to the correct extractor based on archive type. Returns the number
    /// of extractions successfully spawned.
    pub(super) async fn spawn_extractions(
        &mut self,
        job_id: JobId,
        archives: &[(String, crate::jobs::assembly::ArchiveType)],
    ) -> usize {
        let mut spawned = 0;
        for (name, archive_type) in archives {
            self.inflight_extractions
                .entry(job_id)
                .or_default()
                .insert(name.clone());

            let result = match archive_type {
                crate::jobs::assembly::ArchiveType::SevenZip => {
                    self.extract_7z_set(job_id, name).await
                }
                crate::jobs::assembly::ArchiveType::Rar => self.extract_rar_set(job_id, name).await,
                crate::jobs::assembly::ArchiveType::Zip => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Zip)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Tar => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Tar)
                        .await
                }
                crate::jobs::assembly::ArchiveType::TarGz => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::TarGz)
                        .await
                }
                crate::jobs::assembly::ArchiveType::TarBz2 => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::TarBz2)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Gz => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Gz)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Deflate => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Deflate)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Brotli => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Brotli)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Zstd => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Zstd)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Bzip2 => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Bzip2)
                        .await
                }
                crate::jobs::assembly::ArchiveType::Split => {
                    self.extract_simple_archive(job_id, name, SimpleArchiveKind::Split)
                        .await
                }
            };
            match result {
                Ok(_) => spawned += 1,
                Err(e) => {
                    warn!(job_id = job_id.0, archive = %name, error = %e, "failed to start extraction");
                    if let Some(inflight) = self.inflight_extractions.get_mut(&job_id) {
                        inflight.remove(name);
                    }
                }
            }
        }
        spawned
    }

    /// Extract a single 7z archive set. Only collects files belonging to the named set.
    pub(crate) async fn extract_7z_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let (file_paths, password) = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state
                .assembly
                .archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for set '{set_name}'"))?;

            // Collect files belonging to this set using the topology's volume_map.
            let set_filenames: std::collections::HashSet<&str> =
                topo.volume_map.keys().map(|s| s.as_str()).collect();
            let mut parts: Vec<(u32, PathBuf)> = Vec::new();

            for file_asm in state.assembly.files() {
                if set_filenames.contains(file_asm.filename()) {
                    let vol = topo
                        .volume_map
                        .get(file_asm.filename())
                        .copied()
                        .unwrap_or(0);
                    if let Some(path) = self.resolve_job_input_path(job_id, file_asm.filename()) {
                        parts.push((vol, path));
                    }
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            let paths: Vec<PathBuf> = parts.into_iter().map(|(_, p)| p).collect();
            (paths, state.spec.password.clone())
        };

        let output_dir = self.extraction_staging_dir(job_id);
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();

        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_for_channel = set_name.to_string();
        let pp_pool = self.pp_pool.clone();
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                pp_pool.install(move || {
                    if file_paths.is_empty() {
                        return Err(format!("no 7z files found for set '{set_name_owned}'"));
                    }

                    let pw = if let Some(ref p) = password {
                        sevenz_rust2::Password::new(p)
                    } else {
                        sevenz_rust2::Password::empty()
                    };

                    let mut extracted_members = Vec::new();
                    let extracted_members_ref = &mut extracted_members;
                    let event_tx_ref = &event_tx;
                    let output_dir_ref = &output_dir;

                    let extract_fn = |entry: &sevenz_rust2::ArchiveEntry,
                                      reader: &mut dyn std::io::Read,
                                      _dest: &PathBuf|
                     -> Result<bool, sevenz_rust2::Error> {
                        if entry.is_directory() {
                            let dir_path = output_dir_ref.join(entry.name());
                            std::fs::create_dir_all(&dir_path)?;
                            return Ok(true);
                        }

                        let out_path = output_dir_ref.join(entry.name());
                        if let Some(parent) = out_path.parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        let _ = event_tx_ref.send(PipelineEvent::ExtractionMemberStarted {
                            job_id,
                            set_name: set_name_owned.clone(),
                            member: entry.name().to_string(),
                        });

                        let mut file = std::fs::File::create(&out_path)?;
                        let bytes_written = std::io::copy(reader, &mut file)?;

                        tracing::info!(
                            job_id = job_id.0,
                            member = entry.name(),
                            bytes_written,
                            total_bytes = entry.size(),
                            "member extracted"
                        );
                        let _ = event_tx_ref.send(PipelineEvent::ExtractionProgress {
                            job_id,
                            member: entry.name().to_string(),
                            bytes_written,
                            total_bytes: entry.size(),
                        });
                        let _ = event_tx_ref.send(PipelineEvent::ExtractionMemberFinished {
                            job_id,
                            set_name: set_name_owned.clone(),
                            member: entry.name().to_string(),
                        });

                        extracted_members_ref.push(entry.name().to_string());
                        Ok(true)
                    };

                    if file_paths.len() == 1 {
                        let file = std::fs::File::open(&file_paths[0])
                            .map_err(|e| format!("failed to open 7z file: {e}"))?;
                        sevenz_rust2::decompress_with_extract_fn_and_password(
                            file,
                            &output_dir,
                            pw,
                            extract_fn,
                        )
                        .map_err(|e| format!("7z extraction failed: {e}"))?;
                    } else {
                        let reader = crate::pipeline::archive::split_reader::SplitFileReader::open(
                            &file_paths,
                        )
                        .map_err(|e| format!("failed to open 7z split files: {e}"))?;
                        sevenz_rust2::decompress_with_extract_fn_and_password(
                            reader,
                            &output_dir,
                            pw,
                            extract_fn,
                        )
                        .map_err(|e| format!("7z extraction failed: {e}"))?;
                    }

                    Ok(FullSetExtractionOutcome {
                        extracted: extracted_members,
                        failed: Vec::new(),
                    })
                })
            })
            .await;

            let result = match result {
                Ok(r) => r,
                Err(e) => Err(format!("7z extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_for_channel,
                    result,
                })
                .await;
        });

        // Return Ok(0) for now — actual result comes through the channel.
        Ok(0)
    }

    /// Extract a simple (non-RAR, non-7z) archive: ZIP, tar, tar.gz, tar.bz2, gz, deflate, br,
    /// zstd, bz2, or split.
    pub(crate) async fn extract_simple_archive(
        &mut self,
        job_id: JobId,
        set_name: &str,
        kind: SimpleArchiveKind,
    ) -> Result<u32, String> {
        let (file_paths, password) = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state
                .assembly
                .archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for set '{set_name}'"))?;

            let set_filenames: std::collections::HashSet<&str> =
                topo.volume_map.keys().map(|s| s.as_str()).collect();
            let mut parts: Vec<(u32, std::path::PathBuf)> = Vec::new();

            for file_asm in state.assembly.files() {
                if set_filenames.contains(file_asm.filename()) {
                    let vol = topo
                        .volume_map
                        .get(file_asm.filename())
                        .copied()
                        .unwrap_or(0);
                    if let Some(path) = self.resolve_job_input_path(job_id, file_asm.filename()) {
                        parts.push((vol, path));
                    }
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            let paths: Vec<std::path::PathBuf> = parts.into_iter().map(|(_, p)| p).collect();
            (paths, state.spec.password.clone())
        };

        let output_dir = self.extraction_staging_dir(job_id);
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();
        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_for_channel = set_name.to_string();
        let pp_pool = self.pp_pool.clone();

        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                pp_pool.install(move || {
                    if file_paths.is_empty() {
                        return Err(format!("no files found for set '{set_name_owned}'"));
                    }

                    let extracted_members = match kind {
                        SimpleArchiveKind::Zip => extract_zip(
                            &file_paths[0],
                            &output_dir,
                            password.as_deref(),
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Tar => extract_tar(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::TarGz => extract_tar_gz(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::TarBz2 => extract_tar_bz2(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Gz => extract_gz(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Deflate => extract_deflate(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Brotli => extract_brotli(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Zstd => extract_zstd(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Bzip2 => extract_bzip2(
                            &file_paths[0],
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                        SimpleArchiveKind::Split => extract_split(
                            &file_paths,
                            &output_dir,
                            &event_tx,
                            job_id,
                            &set_name_owned,
                        )?,
                    };

                    Ok(FullSetExtractionOutcome {
                        extracted: extracted_members,
                        failed: Vec::new(),
                    })
                })
            })
            .await;

            let result = match result {
                Ok(r) => r,
                Err(e) => Err(format!("{kind:?} extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_for_channel,
                    result,
                })
                .await;
        });

        Ok(0)
    }

    /// Persist RAR volume eligibility without deleting source volumes.
    pub(crate) fn try_delete_volumes(&mut self, job_id: JobId, set_name: &str) {
        let key = (job_id, set_name.to_string());
        let Some(plan) = self.rar_sets.get(&key).and_then(|state| state.plan.clone()) else {
            return;
        };
        let volumes: Vec<u32> = self
            .rar_sets
            .get(&key)
            .map(|state| state.facts.keys().copied().collect())
            .unwrap_or_default();
        if volumes.is_empty() {
            return;
        }
        let verified_suspect = self
            .rar_sets
            .get(&key)
            .map(|state| state.verified_suspect_volumes.clone())
            .unwrap_or_default();
        let mut deleted_now = Vec::new();
        let mut ownership_ready = Vec::new();

        for volume in volumes {
            let Some(decision) = plan.delete_decisions.get(&volume) else {
                continue;
            };
            let Some(filename) =
                Self::rar_volume_filename(&plan.topology.volume_map, volume).map(str::to_string)
            else {
                debug!(
                    job_id = job_id.0,
                    set_name, volume, "RAR eager delete skipped: no filename for volume"
                );
                continue;
            };

            let claim_clean = Self::claim_clean_rar_volume(decision);
            let verification_blocked = verified_suspect.contains(&volume);
            let solid_blocked = plan.is_solid;
            let waiting_on_retry = plan.waiting_on_volumes.contains(&volume);
            let failed_member_claim = !decision.failed_owners.is_empty();
            let already_deleted = self
                .eagerly_deleted
                .get(&job_id)
                .is_some_and(|deleted| deleted.contains(&filename));
            let should_delete = decision.ownership_eligible
                && !waiting_on_retry
                && !failed_member_claim
                && !verification_blocked
                && !solid_blocked
                && !already_deleted;

            if should_delete {
                let Some(path) = self.resolve_job_input_path(job_id, &filename) else {
                    return;
                };
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        self.eagerly_deleted
                            .entry(job_id)
                            .or_default()
                            .insert(filename.clone());
                        deleted_now.push(volume);
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume,
                            file = %filename,
                            owners = ?decision.owners,
                            "RAR volume eagerly deleted"
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume,
                            file = %filename,
                            owners = ?decision.owners,
                            "RAR eager delete found volume already missing"
                        );
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume,
                            file = %filename,
                            owners = ?decision.owners,
                            error = %error,
                            "RAR eager delete failed"
                        );
                    }
                }
            } else {
                let mut reasons = Vec::new();
                if !decision.pending_owners.is_empty() {
                    reasons.push(format!("pending_members={:?}", decision.pending_owners));
                }
                if !decision.failed_owners.is_empty() {
                    reasons.push(format!("failed_members={:?}", decision.failed_owners));
                }
                if decision.unresolved_boundary {
                    reasons.push("unresolved_boundary".to_string());
                }
                if waiting_on_retry {
                    reasons.push("waiting_on_retry".to_string());
                }
                if failed_member_claim {
                    reasons.push("failed_member_claim".to_string());
                }
                if solid_blocked {
                    reasons.push("solid_archive".to_string());
                }
                if !claim_clean {
                    reasons.push("claims_not_clean".to_string());
                }
                if verification_blocked {
                    reasons.push("verified_suspect".to_string());
                }
                if already_deleted {
                    reasons.push("already_deleted".to_string());
                }
                if decision.ownership_eligible && !waiting_on_retry && !failed_member_claim {
                    ownership_ready.push(volume);
                }
                debug!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volume,
                    file = %filename,
                    owners = ?decision.owners,
                    clean_owners = ?decision.clean_owners,
                    failed_owners = ?decision.failed_owners,
                    pending_owners = ?decision.pending_owners,
                    reasons = ?reasons,
                    "RAR eager delete retained volume"
                );
            }

            let deleted = self
                .eagerly_deleted
                .get(&job_id)
                .is_some_and(|deleted| deleted.contains(&filename));
            let par2_clean = claim_clean && !verification_blocked;
            let set_name_owned = set_name.to_string();
            let eligible = decision.ownership_eligible;
            self.db_fire_and_forget(move |db| {
                if let Err(error) = db.set_volume_status(
                    job_id,
                    &set_name_owned,
                    volume,
                    eligible,
                    par2_clean,
                    deleted,
                ) {
                    tracing::error!(
                        job_id = job_id.0,
                        volume,
                        error = %error,
                        "failed to persist RAR volume eligibility"
                    );
                }
            });
        }

        info!(
            job_id = job_id.0,
            set_name = %set_name,
            solid = plan.is_solid,
            ownership_ready = ?ownership_ready,
            deleted_now = ?deleted_now,
            verified_suspect_volumes = ?verified_suspect,
            "RAR eager delete audit"
        );
    }
}
