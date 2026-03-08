use super::*;

impl Pipeline {
    /// If the completed file is a PAR2 index, read it from disk, parse it,
    /// and attach PAR2 metadata to the job assembly for incremental verification.
    pub(super) async fn try_load_par2_metadata(&mut self, job_id: JobId, file_id: NzbFileId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            return;
        };

        // Only process PAR2 index files.
        let is_par2_index = matches!(
            file_asm.role(),
            weaver_core::classify::FileRole::Par2 { is_index: true, .. }
        );
        if !is_par2_index {
            return;
        }

        let filename = file_asm.filename().to_string();
        let file_path = self.output_dir.join(&filename);

        // Read the PAR2 file from disk.
        let par2_bytes = match tokio::fs::read(&file_path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to read PAR2 index file");
                return;
            }
        };

        // Parse PAR2 packets.
        let par2_set = match weaver_par2::Par2FileSet::from_files(&[&par2_bytes]) {
            Ok(set) => set,
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 index");
                return;
            }
        };

        // Build Par2Metadata from the parsed set.
        let mut file_checksums = std::collections::HashMap::new();
        for file_desc in par2_set.recovery_files() {
            if let Some(checksums) = par2_set.file_checksums(&file_desc.file_id) {
                let pairs: Vec<(u32, [u8; 16])> = checksums
                    .iter()
                    .map(|sc| (sc.crc32, sc.md5))
                    .collect();
                file_checksums.insert(file_desc.filename.clone(), pairs);
            }
        }

        let metadata = weaver_assembly::Par2Metadata {
            slice_size: par2_set.slice_size,
            recovery_block_count: par2_set.recovery_block_count(),
            file_checksums,
        };

        let slice_size = metadata.slice_size;
        let recovery_block_count = metadata.recovery_block_count;

        info!(
            job_id = job_id.0,
            filename = %filename,
            slice_size,
            recovery_blocks = recovery_block_count,
            "PAR2 metadata loaded"
        );

        // Retain the Par2FileSet for repair (avoids re-reading from disk).
        self.par2_sets.insert(job_id, Arc::new(par2_set));

        // Attach to job assembly.
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        state.assembly.set_par2_metadata(metadata);

        let _ = self
            .event_tx
            .send(PipelineEvent::Par2MetadataLoaded { job_id });

        // Journal entry.
        let journal_entry = JournalEntry::Par2MetadataLoaded {
            job_id,
            slice_size,
            recovery_block_count,
        };
        if let Err(e) = self.journal.append(&journal_entry).await {
            error!(error = %e, "journal write failed");
        }
    }

    /// When a PAR2 recovery volume completes, parse it and merge recovery
    /// slices into the retained Par2FileSet (avoids re-reading at repair time).
    pub(super) async fn try_merge_par2_recovery(&mut self, job_id: JobId, file_id: NzbFileId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            return;
        };

        // Only process PAR2 recovery volumes (not the index).
        let is_par2_volume = matches!(
            file_asm.role(),
            weaver_core::classify::FileRole::Par2 { is_index: false, .. }
        );
        if !is_par2_volume {
            return;
        }

        // Need a retained Par2FileSet to merge into.
        if !self.par2_sets.contains_key(&job_id) {
            return;
        }

        let filename = file_asm.filename().to_string();
        let file_path = self.output_dir.join(&filename);

        let par2_bytes = match tokio::fs::read(&file_path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to read PAR2 recovery volume");
                return;
            }
        };

        // Parse packets from this volume file.
        let packets = weaver_par2::packet::scan_packets(&par2_bytes, 0);
        let packet_list: Vec<_> = packets.into_iter().map(|(p, _)| p).collect();

        // Merge into the retained set (Arc::make_mut clones only if shared).
        let par2_set = Arc::make_mut(self.par2_sets.get_mut(&job_id).unwrap());
        match par2_set.merge_packets(packet_list) {
            Ok(result) if result.new_recovery_slices > 0 => {
                info!(
                    job_id = job_id.0,
                    filename = %filename,
                    recovery_blocks_merged = result.new_recovery_slices,
                    total_recovery = par2_set.recovery_block_count(),
                    "merged PAR2 recovery volume"
                );
            }
            Err(e) => {
                warn!(
                    job_id = job_id.0,
                    filename = %filename,
                    error = %e,
                    "failed to merge PAR2 recovery volume"
                );
            }
            _ => {}
        }
    }

    /// When a RAR volume file completes, parse its headers to build or update
    /// the archive topology on the job assembly.
    pub(super) async fn try_update_archive_topology(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (volume_number, filename) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };

            let vol = match file_asm.role() {
                weaver_core::classify::FileRole::RarVolume { volume_number } => *volume_number,
                _ => return,
            };
            (vol, file_asm.filename().to_string())
        };

        let file_path = self.output_dir.join(&filename);

        // Only parse headers from the first volume (volume 0) to build topology.
        // For subsequent volumes, just mark them complete.
        if volume_number == 0 {
            let path = file_path.clone();
            let topo_result = tokio::task::spawn_blocking(move || {
                let file = std::fs::File::open(&path)?;
                let archive = weaver_rar::RarArchive::open(file)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                let meta = archive.metadata();
                Ok::<_, std::io::Error>(meta)
            })
            .await;

            match topo_result {
                Ok(Ok(meta)) => {
                    let mut volume_map = std::collections::HashMap::new();
                    let mut members = Vec::new();

                    // Map filenames in the NZB to volume numbers.
                    if let Some(state) = self.jobs.get(&job_id) {
                        for file_asm in state.assembly.files() {
                            if let weaver_core::classify::FileRole::RarVolume { volume_number: vn } =
                                file_asm.role()
                            {
                                volume_map
                                    .insert(file_asm.filename().to_string(), *vn);
                            }
                        }
                    }

                    for member in &meta.members {
                        if member.is_directory {
                            continue;
                        }
                        members.push(weaver_assembly::ArchiveMember {
                            name: member.name.clone(),
                            first_volume: member.volumes.first_volume as u32,
                            last_volume: member.volumes.last_volume as u32,
                            unpacked_size: member.unpacked_size.unwrap_or(0),
                        });
                    }

                    let expected_volume_count = meta.volume_count.map(|c| c as u32);

                    let topology = weaver_assembly::ArchiveTopology {
                        volume_map,
                        complete_volumes: std::collections::HashSet::new(),
                        expected_volume_count,
                        members,
                    };

                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state.assembly.set_archive_topology(topology);
                        // Mark this volume as complete.
                        state.assembly.mark_volume_complete(volume_number);

                        // Retroactively mark volumes that completed before
                        // topology was available. Without this, volumes that
                        // finished before volume 0 are never tracked.
                        let completed_volumes: Vec<u32> = state.assembly.files()
                            .filter(|f| f.is_complete())
                            .filter_map(|f| match f.role() {
                                weaver_core::classify::FileRole::RarVolume { volume_number: vn } => Some(*vn),
                                _ => None,
                            })
                            .collect();
                        for vn in &completed_volumes {
                            state.assembly.mark_volume_complete(*vn);
                        }
                    }

                    info!(
                        job_id = job_id.0,
                        volume = volume_number,
                        member_count = meta.members.len(),
                        "archive topology loaded from first RAR volume"
                    );
                }
                Ok(Err(e)) => {
                    warn!(
                        job_id = job_id.0,
                        filename = %filename,
                        error = %e,
                        "failed to parse RAR volume headers"
                    );
                }
                Err(e) => {
                    warn!(
                        job_id = job_id.0,
                        error = %e,
                        "RAR header parsing task panicked"
                    );
                }
            }
        } else {
            // Non-first volume: just mark it complete.
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.assembly.mark_volume_complete(volume_number);
            }
            debug!(
                job_id = job_id.0,
                volume = volume_number,
                "RAR volume complete"
            );
        }
    }

    /// Re-read and verify slices that weren't verified incrementally during download.
    /// This happens when segments arrived before PAR2 metadata was loaded — their data
    /// was dropped via commit_segment_meta() without feeding slice checksums.
    /// Only reads the specific byte ranges of unverified slices, not entire files.
    pub(super) async fn verify_unverified_slices_from_disk(&mut self, job_id: JobId) {
        // Collect unverified slices across all files: (file_id, filename, slice descriptors).
        let unverified: Vec<(NzbFileId, String, Vec<(u32, u64, u64, u32, [u8; 16])>)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let par2_slice_size = state
                .assembly
                .par2_metadata()
                .map(|m| m.slice_size)
                .unwrap_or(0);
            if par2_slice_size == 0 {
                return;
            }
            state
                .assembly
                .files()
                .filter_map(|f| {
                    let slices = f.unverified_slices();
                    if slices.is_empty() {
                        None
                    } else {
                        Some((f.file_id(), f.filename().to_owned(), slices))
                    }
                })
                .collect()
        };

        if unverified.is_empty() {
            debug!(job_id = job_id.0, "all slices verified incrementally");
            return;
        }

        let total_unverified: usize = unverified.iter().map(|(_, _, s)| s.len()).sum();
        info!(
            job_id = job_id.0,
            unverified_slices = total_unverified,
            files = unverified.len(),
            "re-verifying slices from disk (segments arrived before PAR2 metadata)"
        );

        let par2_slice_size = self
            .jobs
            .get(&job_id)
            .and_then(|s| s.assembly.par2_metadata())
            .map(|m| m.slice_size)
            .unwrap_or(0);

        for (file_id, filename, slices) in unverified {
            let file_path = self.output_dir.join(&filename);
            let slice_size = par2_slice_size;

            // Read and verify each unverified slice from disk.
            let results: Vec<(u32, bool)> = match tokio::task::spawn_blocking({
                let file_path = file_path.clone();
                move || -> Result<Vec<(u32, bool)>, std::io::Error> {
                    use std::io::{Read, Seek, SeekFrom};
                    let mut file = std::fs::File::open(&file_path)?;
                    let file_len = file.metadata()?.len();

                    let mut results = Vec::with_capacity(slices.len());
                    for (slice_idx, byte_start, byte_end, expected_crc, expected_md5) in slices {
                        if byte_start >= file_len {
                            results.push((slice_idx, false));
                            continue;
                        }
                        let read_end = byte_end.min(file_len);
                        let read_len = (read_end - byte_start) as usize;
                        let mut buf = vec![0u8; read_len];
                        file.seek(SeekFrom::Start(byte_start))?;
                        file.read_exact(&mut buf)?;

                        let mut state = checksum::SliceChecksumState::new();
                        state.update(&buf);

                        // Pad last slice if needed (PAR2 spec).
                        let pad_to = if (read_len as u64) < slice_size {
                            Some(slice_size)
                        } else {
                            None
                        };
                        let (crc, md5) = state.finalize(pad_to);
                        let valid = crc == expected_crc && md5 == expected_md5;
                        results.push((slice_idx, valid));
                    }
                    Ok(results)
                }
            })
            .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    warn!(
                        file = %filename,
                        error = %e,
                        "failed to re-verify slices from disk"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(
                        file = %filename,
                        error = %e,
                        "slice re-verification task panicked"
                    );
                    continue;
                }
            };

            // Apply results back to assembly.
            let state = self.jobs.get_mut(&job_id).unwrap();
            let file_asm = state.assembly.file_mut(file_id).unwrap();
            let mut damaged_count = 0u32;
            for (slice_idx, valid) in &results {
                file_asm.mark_slice_verified(*slice_idx, *valid);
                if !valid {
                    damaged_count += 1;
                    info!(
                        file = %filename,
                        slice = slice_idx,
                        "damaged slice detected during disk re-verification"
                    );
                }
            }
            info!(
                file = %filename,
                verified = results.len(),
                damaged = damaged_count,
                "disk re-verification complete"
            );
        }
    }
}
