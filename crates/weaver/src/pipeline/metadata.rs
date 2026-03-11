use super::*;

impl Pipeline {
    /// If the completed file is a PAR2 index, read it from disk, parse it,
    /// and retain the Par2FileSet for repair. For obfuscated uploads, remap
    /// PAR2 file descriptions to use deobfuscated filenames (matched by volume number).
    pub(super) async fn try_load_par2_metadata(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (filename, file_path, is_par2, is_index) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };

            let is_par2 = matches!(
                file_asm.role(),
                weaver_core::classify::FileRole::Par2 { .. }
            );
            let is_index = matches!(
                file_asm.role(),
                weaver_core::classify::FileRole::Par2 { is_index: true, .. }
            );
            let filename = file_asm.filename().to_string();
            let file_path = state.working_dir.join(&filename);
            (filename, file_path, is_par2, is_index)
        };

        if !is_par2 {
            return;
        }
        // If this is a recovery volume and we already have PAR2 metadata,
        // let try_merge_par2_recovery handle it instead. But if no par2_set
        // exists yet (no index file), treat this recovery volume as the
        // initial source of PAR2 metadata — every PAR2 file contains the
        // file descriptions and checksums needed for verification.
        if !is_index && self.par2_sets.contains_key(&job_id) {
            return;
        }

        // Read the PAR2 file from disk.
        let par2_bytes = match tokio::fs::read(&file_path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to read PAR2 index file");
                return;
            }
        };

        // Parse PAR2 packets.
        let mut par2_set = match weaver_par2::Par2FileSet::from_files(&[&par2_bytes]) {
            Ok(set) => set,
            Err(e) => {
                warn!(filename = %filename, error = %e, "failed to parse PAR2 index");
                return;
            }
        };

        // Check if PAR2 filenames match assembly filenames (obfuscation check).
        // Obfuscated uploads use random base names in PAR2 (e.g.,
        // "6OLN1UG33OBhAM9rAfSUdX.part001.rar") while the NZB/assembly uses
        // the deobfuscated names. Remap Par2FileSet descriptions so that
        // DiskFileAccess will find the correct files at repair time.
        let has_matches = self.jobs.get(&job_id).is_some_and(|state| {
            par2_set.files.values().any(|desc| {
                state
                    .assembly
                    .files()
                    .any(|f| f.filename() == desc.filename)
            })
        });

        if !has_matches && !par2_set.files.is_empty() {
            // Build volume_number → deobfuscated filename from assembly.
            let mut assembly_by_volume: std::collections::HashMap<u32, String> =
                std::collections::HashMap::new();
            if let Some(state) = self.jobs.get(&job_id) {
                for file_asm in state.assembly.files() {
                    if let weaver_core::classify::FileRole::RarVolume { volume_number } =
                        file_asm.role()
                    {
                        assembly_by_volume.insert(*volume_number, file_asm.filename().to_string());
                    }
                }
            }

            // Remap PAR2 file descriptions: match by volume number, replace filename.
            let mut remapped = 0u32;
            for desc in par2_set.files.values_mut() {
                let role = weaver_core::classify::FileRole::from_filename(&desc.filename);
                if let weaver_core::classify::FileRole::RarVolume { volume_number } = role {
                    if let Some(deobfuscated) = assembly_by_volume.get(&volume_number) {
                        desc.filename = deobfuscated.clone();
                        remapped += 1;
                    }
                }
            }

            if remapped > 0 {
                info!(
                    job_id = job_id.0,
                    remapped,
                    total = par2_set.files.len(),
                    "PAR2 filenames obfuscated — remapped by volume number"
                );
            } else {
                warn!(
                    job_id = job_id.0,
                    "PAR2 filenames don't match assembly and volume-number matching failed"
                );
            }
        }

        let slice_size = par2_set.slice_size;
        let recovery_block_count = par2_set.recovery_block_count();

        self.note_recovery_block_count(job_id, file_id.file_index, recovery_block_count);

        info!(
            job_id = job_id.0,
            filename = %filename,
            slice_size,
            recovery_blocks = recovery_block_count,
            "PAR2 metadata loaded"
        );

        // Retain the Par2FileSet for repair (avoids re-reading from disk).
        self.par2_sets.insert(job_id, Arc::new(par2_set));

        let _ = self
            .event_tx
            .send(PipelineEvent::Par2MetadataLoaded { job_id });

        // Persist PAR2 metadata to SQLite.
        if let Err(e) = self
            .db
            .set_par2_metadata(job_id, slice_size, recovery_block_count)
        {
            error!(error = %e, "db write failed for set_par2_metadata");
        }
    }

    /// When a PAR2 recovery volume completes, parse it and merge recovery
    /// slices into the retained Par2FileSet (avoids re-reading at repair time).
    pub(super) async fn try_merge_par2_recovery(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (filename, file_path, is_par2_volume) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };

            let is_par2_volume = matches!(
                file_asm.role(),
                weaver_core::classify::FileRole::Par2 {
                    is_index: false,
                    ..
                }
            );
            let filename = file_asm.filename().to_string();
            let file_path = state.working_dir.join(&filename);
            (filename, file_path, is_par2_volume)
        };
        if !is_par2_volume {
            return;
        }

        // Need a retained Par2FileSet to merge into.
        if !self.par2_sets.contains_key(&job_id) {
            return;
        }

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
        let merge_result = {
            let par2_set = Arc::make_mut(self.par2_sets.get_mut(&job_id).unwrap());
            let merge = par2_set.merge_packets(packet_list);
            let total_recovery = par2_set.recovery_block_count();
            (merge, total_recovery)
        };
        match merge_result {
            (Ok(result), total_recovery) if result.new_recovery_slices > 0 => {
                self.note_recovery_block_count(
                    job_id,
                    file_id.file_index,
                    result.new_recovery_slices,
                );
                info!(
                    job_id = job_id.0,
                    filename = %filename,
                    recovery_blocks_merged = result.new_recovery_slices,
                    total_recovery,
                    "merged PAR2 recovery volume"
                );
            }
            (Err(e), _) => {
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
        let (volume_number, filename, password, working_dir) = {
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
            (
                vol,
                file_asm.filename().to_string(),
                state.spec.password.clone(),
                state.working_dir.clone(),
            )
        };

        // Derive set name from filename base (e.g., "movie" from "movie.part01.rar").
        let set_name = weaver_core::classify::archive_base_name(
            &filename,
            &weaver_core::classify::FileRole::RarVolume { volume_number },
        )
        .unwrap_or_else(|| "rar".into());

        let file_path = working_dir.join(&filename);

        // Only parse headers from the first volume (volume 0) to build topology.
        // For subsequent volumes, just mark them complete.
        if volume_number == 0 {
            let path = file_path.clone();
            info!(job_id = job_id.0, filename = %filename, "parsing RAR first volume headers (spawn_blocking)");
            let topo_result = tokio::task::spawn_blocking(move || {
                tracing::info!("RAR parse: opening file {:?}", path);
                let file = std::fs::File::open(&path)?;
                let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
                tracing::info!("RAR parse: file opened, size={}", file_len);
                let archive = if let Some(ref pw) = password {
                    weaver_rar::RarArchive::open_with_password(file, pw)
                } else {
                    weaver_rar::RarArchive::open(file)
                }
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                tracing::info!("RAR parse: archive opened, getting metadata");
                let meta = archive.metadata();
                tracing::info!(
                    "RAR parse: done, members={} volumes={:?}",
                    meta.members.len(),
                    meta.volume_count
                );
                Ok::<_, std::io::Error>(meta)
            })
            .await;
            info!(job_id = job_id.0, "RAR first volume parse complete");

            match topo_result {
                Ok(Ok(meta)) => {
                    let mut volume_map = std::collections::HashMap::new();
                    let mut members = Vec::new();

                    // Map filenames in the NZB to volume numbers — only those
                    // belonging to the same archive set (matching base name).
                    if let Some(state) = self.jobs.get(&job_id) {
                        for file_asm in state.assembly.files() {
                            if let weaver_core::classify::FileRole::RarVolume {
                                volume_number: vn,
                            } = file_asm.role()
                            {
                                let f_base = weaver_core::classify::archive_base_name(
                                    file_asm.filename(),
                                    file_asm.role(),
                                );
                                if f_base.as_deref() == Some(&set_name) {
                                    volume_map.insert(file_asm.filename().to_string(), *vn);
                                }
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
                        archive_type: weaver_assembly::ArchiveType::Rar,
                        volume_map,
                        complete_volumes: std::collections::HashSet::new(),
                        expected_volume_count,
                        members,
                    };

                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state
                            .assembly
                            .set_archive_topology(set_name.clone(), topology);
                        state
                            .assembly
                            .mark_volume_complete(&set_name, volume_number);

                        // Retroactively mark volumes that completed before
                        // topology was available (same set only).
                        let completed_volumes: Vec<u32> = state
                            .assembly
                            .files()
                            .filter(|f| f.is_complete())
                            .filter_map(|f| match f.role() {
                                weaver_core::classify::FileRole::RarVolume {
                                    volume_number: vn,
                                } => {
                                    let f_base = weaver_core::classify::archive_base_name(
                                        f.filename(),
                                        f.role(),
                                    );
                                    if f_base.as_deref() == Some(&set_name) {
                                        Some(*vn)
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            })
                            .collect();
                        for vn in &completed_volumes {
                            state.assembly.mark_volume_complete(&set_name, *vn);
                        }
                    }

                    info!(
                        job_id = job_id.0,
                        volume = volume_number,
                        set_name = %set_name,
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
            // Non-first volume: mark it complete.
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state
                    .assembly
                    .mark_volume_complete(&set_name, volume_number);
            }

            // Update last_volume for the member whose data continues through
            // this volume. In a sequential multi-file RAR archive, the member
            // with the highest first_volume <= volume_number is the one whose
            // data fills this volume. This must happen for EVERY completed
            // volume so deletable_volumes() knows the full span of each member.
            if let Some(state) = self.jobs.get_mut(&job_id)
                && let Some(topo) = state.assembly.archive_topology_for_mut(&set_name)
            {
                if let Some(continuing) = topo
                    .members
                    .iter_mut()
                    .filter(|m| m.first_volume <= volume_number)
                    .max_by_key(|m| m.first_volume)
                {
                    if volume_number > continuing.last_volume {
                        continuing.last_volume = volume_number;
                    }
                }
            }

            // Parse headers to discover new members starting in this volume.
            // (e.g., S01E02 starts after S01E01's data ends in a later volume.)
            let has_topology = self
                .jobs
                .get(&job_id)
                .is_some_and(|s| s.assembly.archive_topology_for(&set_name).is_some());

            if has_topology {
                let path = file_path.clone();
                let pw = password.clone();
                let parse_result = tokio::task::spawn_blocking(move || {
                    let file = std::fs::File::open(&path)?;
                    let archive = if let Some(ref pw) = pw {
                        weaver_rar::RarArchive::open_with_password(file, pw)
                    } else {
                        weaver_rar::RarArchive::open(file)
                    }
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                    Ok::<_, std::io::Error>(archive.metadata())
                })
                .await;

                match parse_result {
                    Ok(Ok(meta)) => {
                        if let Some(state) = self.jobs.get_mut(&job_id)
                            && let Some(topo) = state.assembly.archive_topology_for_mut(&set_name)
                        {
                            let existing_names: std::collections::HashSet<String> =
                                topo.members.iter().map(|m| m.name.clone()).collect();
                            let mut added = 0u32;
                            for member in &meta.members {
                                if member.is_directory {
                                    continue;
                                }
                                let name = weaver_rar::sanitize_path(&member.name);
                                if existing_names.contains(&name) {
                                    continue;
                                }
                                topo.members.push(weaver_assembly::ArchiveMember {
                                    name,
                                    first_volume: volume_number,
                                    last_volume: volume_number,
                                    unpacked_size: member.unpacked_size.unwrap_or(0),
                                });
                                added += 1;
                            }
                            if added > 0 {
                                info!(
                                    job_id = job_id.0,
                                    volume = volume_number,
                                    set_name = %set_name,
                                    added,
                                    "discovered new archive members from later volume"
                                );
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        debug!(
                            job_id = job_id.0,
                            volume = volume_number,
                            error = %e,
                            "failed to parse later RAR volume (non-fatal)"
                        );
                    }
                    Err(e) => {
                        debug!(
                            job_id = job_id.0,
                            error = %e,
                            "RAR volume parse task panicked (non-fatal)"
                        );
                    }
                }
            }

            debug!(
                job_id = job_id.0,
                volume = volume_number,
                "RAR volume complete"
            );
        }
    }

    /// When a 7z file completes, build or update the archive topology.
    ///
    /// Groups files by base name (e.g., "Show.S01E01.7z") so that multiple
    /// independent archive sets within a single job are tracked separately.
    pub(super) fn try_update_7z_topology(&mut self, job_id: JobId, file_id: NzbFileId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            return;
        };

        let role = file_asm.role().clone();
        let filename = file_asm.filename().to_string();
        let set_name = match weaver_core::classify::archive_base_name(&filename, &role) {
            Some(name) => name,
            None => return,
        };

        match role {
            weaver_core::classify::FileRole::SevenZipArchive => {
                // Single .7z file — topology has exactly one volume.
                if state.assembly.archive_topology_for(&set_name).is_some() {
                    return; // Already set.
                }

                let mut volume_map = std::collections::HashMap::new();
                volume_map.insert(filename.clone(), 0);

                let topology = weaver_assembly::ArchiveTopology {
                    archive_type: weaver_assembly::ArchiveType::SevenZip,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(1),
                    members: vec![weaver_assembly::ArchiveMember {
                        name: set_name.clone(),
                        first_volume: 0,
                        last_volume: 0,
                        unpacked_size: 0,
                    }],
                };

                let state = self.jobs.get_mut(&job_id).unwrap();
                state
                    .assembly
                    .set_archive_topology(set_name.clone(), topology);
                state.assembly.mark_volume_complete(&set_name, 0);

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "7z topology set (single archive)"
                );
            }
            weaver_core::classify::FileRole::SevenZipSplit { number } => {
                let completing_number = number;

                if state.assembly.archive_topology_for(&set_name).is_some() {
                    // Topology already exists for this set — just mark this volume complete.
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state
                        .assembly
                        .mark_volume_complete(&set_name, completing_number);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        volume = completing_number,
                        "7z split volume complete"
                    );
                    return;
                }

                // First split part for this set to complete — build topology
                // from all known splits with the same base name.
                let mut volume_map = std::collections::HashMap::new();
                let mut max_number = 0u32;
                for f in state.assembly.files() {
                    if let weaver_core::classify::FileRole::SevenZipSplit { number: n } = f.role() {
                        let f_base =
                            weaver_core::classify::archive_base_name(f.filename(), f.role());
                        if f_base.as_deref() == Some(&set_name) {
                            volume_map.insert(f.filename().to_string(), *n);
                            max_number = max_number.max(*n);
                        }
                    }
                }

                let expected = max_number + 1;
                let topology = weaver_assembly::ArchiveTopology {
                    archive_type: weaver_assembly::ArchiveType::SevenZip,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(expected),
                    members: vec![weaver_assembly::ArchiveMember {
                        name: set_name.clone(),
                        first_volume: 0,
                        last_volume: max_number,
                        unpacked_size: 0,
                    }],
                };

                let state = self.jobs.get_mut(&job_id).unwrap();
                state
                    .assembly
                    .set_archive_topology(set_name.clone(), topology);

                // Retroactively mark all already-complete split parts for this set.
                let completed: Vec<u32> = state
                    .assembly
                    .files()
                    .filter(|f| f.is_complete())
                    .filter_map(|f| {
                        if let weaver_core::classify::FileRole::SevenZipSplit { number: n } =
                            f.role()
                        {
                            let f_base =
                                weaver_core::classify::archive_base_name(f.filename(), f.role());
                            if f_base.as_deref() == Some(&set_name) {
                                return Some(*n);
                            }
                        }
                        None
                    })
                    .collect();
                for n in completed {
                    state.assembly.mark_volume_complete(&set_name, n);
                }

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    expected_volumes = expected,
                    "7z topology set (split archive)"
                );
            }
            _ => {}
        }
    }
}
