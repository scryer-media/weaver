use std::io::BufWriter;

use super::*;

impl Pipeline {
    /// Try partial extraction: extract archive members whose volumes are all present.
    /// Called after each file completes, not just when all files are done.
    pub(super) async fn try_partial_extraction(&mut self, job_id: JobId) {
        // Check for ready archive sets (7z or RAR) — extract each independently.
        let ready_sets: Vec<(String, weaver_assembly::ArchiveType)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .ready_archive_sets()
                .into_iter()
                .filter_map(|set_name| {
                    let topo = state.assembly.archive_topology_for(&set_name)?;
                    if self
                        .extracted_sets
                        .get(&job_id)
                        .is_some_and(|s| s.contains(&set_name))
                    {
                        return None;
                    }
                    Some((set_name, topo.archive_type))
                })
                .collect()
        };

        for (set_name, archive_type) in ready_sets {
            match archive_type {
                weaver_assembly::ArchiveType::SevenZip => {
                    info!(job_id = job_id.0, set_name = %set_name, "extracting ready 7z set");
                    match self.extract_7z_set(job_id, &set_name).await {
                        Ok(count) => {
                            info!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                members = count,
                                "7z set extraction complete"
                            );
                            self.extracted_sets
                                .entry(job_id)
                                .or_default()
                                .insert(set_name);
                        }
                        Err(e) => {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                error = %e,
                                "7z set extraction failed"
                            );
                        }
                    }
                }
                weaver_assembly::ArchiveType::Rar => {
                    info!(job_id = job_id.0, set_name = %set_name, "extracting ready RAR set");
                    match self.extract_rar_set(job_id, &set_name).await {
                        Ok(count) => {
                            info!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                members = count,
                                "RAR set extraction complete"
                            );
                            self.extracted_sets
                                .entry(job_id)
                                .or_default()
                                .insert(set_name);
                        }
                        Err(e) => {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                error = %e,
                                "RAR set extraction failed"
                            );
                        }
                    }
                }
            }
        }

        // RAR streaming/batch extraction for sets that aren't fully ready yet.
        {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let has_rar = state
                .assembly
                .archive_topologies()
                .values()
                .any(|t| t.archive_type == weaver_assembly::ArchiveType::Rar);
            if !has_rar {
                return;
            }
        }

        // Feed active streaming providers for all RAR sets.
        let active_streaming: Vec<(JobId, String)> = self
            .streaming_providers
            .keys()
            .filter(|(jid, _)| *jid == job_id)
            .cloned()
            .collect();
        if !active_streaming.is_empty() {
            for key in &active_streaming {
                self.feed_streaming_volumes(job_id, &key.1);
            }
            return;
        }

        // Try to start streaming extraction for each RAR set.
        if self.try_start_streaming_extraction(job_id).await {
            return;
        }

        // Fall back to the original partial extraction path
        // (multi-member archives where some members are fully ready).
        self.try_batch_extraction(job_id).await;
    }

    /// Start streaming extraction for a job if any RAR set's first volume is ready.
    ///
    /// Returns true if streaming extraction was started (or isn't applicable),
    /// false if we should fall back to batch extraction.
    async fn try_start_streaming_extraction(&mut self, job_id: JobId) -> bool {
        let (set_name, member_name, first_volume) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return true;
            };
            match &state.status {
                JobStatus::Downloading | JobStatus::Verifying => {}
                _ => return true,
            }

            // Find the first RAR set with a ready first volume.
            let mut found = None;
            for (sname, topo) in state.assembly.archive_topologies() {
                if topo.archive_type != weaver_assembly::ArchiveType::Rar {
                    continue;
                }
                for member in &topo.members {
                    if topo.complete_volumes.contains(&member.first_volume) {
                        // Don't stream-extract if already extracted.
                        if self
                            .extracted_members
                            .get(&job_id)
                            .is_some_and(|s| s.contains(&member.name))
                        {
                            continue;
                        }
                        found = Some((sname.clone(), member.name.clone(), member.first_volume));
                        break;
                    }
                }
                if found.is_some() {
                    break;
                }
            }

            match found {
                Some(f) => f,
                None => return false,
            }
        };

        info!(
            job_id = job_id.0,
            set_name = %set_name,
            member = %member_name,
            first_volume,
            "starting streaming extraction"
        );

        // Create the WaitingVolumeProvider and feed all currently-complete volumes.
        let provider = Arc::new(weaver_rar::WaitingVolumeProvider::new());
        let provider_key = (job_id, set_name.clone());
        self.streaming_providers
            .insert(provider_key, Arc::clone(&provider));

        // Feed all currently-complete volumes for this set into the provider.
        self.feed_streaming_volumes(job_id, &set_name);

        // Collect info for the blocking task.
        let state = self.jobs.get(&job_id).unwrap();
        let topo = state.assembly.archive_topology_for(&set_name).unwrap();
        let set_filenames: HashSet<&str> = topo.volume_map.keys().map(|s| s.as_str()).collect();

        let first_vol_path = {
            let mut path = None;
            for file_asm in state.assembly.files() {
                if set_filenames.contains(file_asm.filename())
                    && let weaver_core::classify::FileRole::RarVolume { volume_number } =
                        file_asm.role()
                        && *volume_number == first_volume && file_asm.is_complete() {
                            path = Some(state.working_dir.join(file_asm.filename()));
                            break;
                        }
            }
            match path {
                Some(p) => p,
                None => {
                    warn!(job_id = job_id.0, "first volume path not found");
                    self.streaming_providers
                        .remove(&(job_id, set_name));
                    return false;
                }
            }
        };
        let password = state.spec.password.clone();
        let output_dir = state.working_dir.clone();
        let event_tx = self.event_tx.clone();
        let target_member = member_name.clone();

        // Spawn the blocking extraction task.
        let extraction_provider = Arc::clone(&provider);
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                // Open the first volume to parse headers.
                let first_file = std::fs::File::open(&first_vol_path)
                    .map_err(|e| format!("failed to open first volume: {e}"))?;
                let mut archive = if let Some(ref pw) = password {
                    weaver_rar::RarArchive::open_with_password(first_file, pw)
                } else {
                    weaver_rar::RarArchive::open(first_file)
                }
                .map_err(|e| format!("failed to open RAR archive: {e}"))?;

                let meta = archive.metadata();
                let options = weaver_rar::ExtractOptions {
                    verify: true,
                    password: password.clone(),
                };

                // Find the target member.
                let idx = meta
                    .members
                    .iter()
                    .position(|m| m.name == target_member)
                    .ok_or_else(|| {
                        format!("member {} not found in archive", target_member)
                    })?;

                let member = &meta.members[idx];
                if member.is_directory {
                    let dir_path = output_dir.join(&member.name);
                    std::fs::create_dir_all(&dir_path)
                        .map_err(|e| format!("failed to create dir {}: {e}", member.name))?;
                    return Ok((target_member.clone(), 0u64));
                }

                let out_path = output_dir.join(&member.name);
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("failed to create parent dir: {e}"))?;
                }

                let file = std::fs::File::create(&out_path)
                    .map_err(|e| format!("failed to create output file: {e}"))?;
                let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

                // This call blocks on the VolumeProvider as it reads each volume.
                let bytes_written = archive
                    .extract_member_streaming(
                        idx,
                        &options,
                        &*extraction_provider,
                        &mut writer,
                    )
                    .map_err(|e| format!("streaming extraction failed: {e}"))?;

                let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                    job_id,
                    member: member.name.clone(),
                    bytes_written,
                    total_bytes: member.unpacked_size.unwrap_or(0),
                });

                Ok::<_, String>((target_member.clone(), bytes_written))
            })
            .await;

            match result {
                Ok(Ok((member, bytes))) => {
                    info!(
                        job_id = job_id.0,
                        member = %member,
                        bytes,
                        "streaming extraction complete"
                    );
                }
                Ok(Err(e)) => {
                    warn!(job_id = job_id.0, error = %e, "streaming extraction failed");
                }
                Err(e) => {
                    warn!(
                        job_id = job_id.0,
                        error = %e,
                        "streaming extraction task panicked"
                    );
                }
            }
        });

        // Mark the member as being extracted.
        self.extracted_members
            .entry(job_id)
            .or_default()
            .insert(member_name);

        true
    }

    /// Feed all currently-complete volume paths for a RAR set to its streaming provider.
    fn feed_streaming_volumes(&self, job_id: JobId, set_name: &str) {
        let key = (job_id, set_name.to_string());
        let Some(provider) = self.streaming_providers.get(&key) else {
            return;
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };

        // Only feed volumes belonging to this set.
        let topo = match state.assembly.archive_topology_for(set_name) {
            Some(t) => t,
            None => return,
        };
        let set_filenames: HashSet<&str> =
            topo.volume_map.keys().map(|s| s.as_str()).collect();

        for file_asm in state.assembly.files() {
            if set_filenames.contains(file_asm.filename())
                && let weaver_core::classify::FileRole::RarVolume { volume_number } =
                    file_asm.role()
                    && file_asm.is_complete() {
                        let path = state.working_dir.join(file_asm.filename());
                        provider.volume_ready(*volume_number as usize, path);
                    }
        }

        // If the job is complete/failed, mark the provider as finished.
        match &state.status {
            JobStatus::Complete | JobStatus::Failed { .. } => {
                provider.mark_finished();
            }
            _ => {}
        }
    }

    /// Cancel streaming extraction for a job (called on job removal/failure).
    pub(super) fn cancel_streaming_extraction(&mut self, job_id: JobId) {
        let keys: Vec<(JobId, String)> = self
            .streaming_providers
            .keys()
            .filter(|(jid, _)| *jid == job_id)
            .cloned()
            .collect();
        for key in keys {
            if let Some(provider) = self.streaming_providers.remove(&key) {
                provider.mark_cancelled("job cancelled".into());
            }
        }
    }

    /// Original batch extraction path: extract members whose volumes are all present.
    /// Scoped to a specific RAR set when multiple sets exist.
    async fn try_batch_extraction(&mut self, job_id: JobId) {
        // Collect per-set readiness for RAR sets only.
        let rar_sets: Vec<String> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            match &state.status {
                JobStatus::Downloading | JobStatus::Verifying => {}
                _ => return,
            }
            state
                .assembly
                .archive_topologies()
                .iter()
                .filter(|(_, t)| t.archive_type == weaver_assembly::ArchiveType::Rar)
                .map(|(name, _)| name.clone())
                .collect()
        };

        for set_name in rar_sets {
            let readiness = {
                let state = self.jobs.get(&job_id).unwrap();
                state.assembly.set_extraction_readiness(&set_name)
            };

            let extractable_names = match readiness {
                ExtractionReadiness::Partial { extractable, .. } => extractable,
                _ => continue,
            };

            // Filter out already-extracted members.
            let already_extracted = self.extracted_members.entry(job_id).or_default();
            let new_extractable: Vec<String> = extractable_names
                .into_iter()
                .filter(|name| !already_extracted.contains(name))
                .collect();

            if new_extractable.is_empty() {
                continue;
            }

            info!(
                job_id = job_id.0,
                set_name = %set_name,
                members = ?new_extractable,
                "batch extraction: extracting ready members"
            );

            // Collect volume paths scoped to this set.
            let (volume_paths, password, working_dir) = {
                let state = self.jobs.get(&job_id).unwrap();
                let topo = match state.assembly.archive_topology_for(&set_name) {
                    Some(t) => t,
                    None => continue,
                };
                let set_filenames: HashSet<&str> =
                    topo.volume_map.keys().map(|s| s.as_str()).collect();

                let mut vols: Vec<(u32, PathBuf)> = Vec::new();
                for file_asm in state.assembly.files() {
                    if set_filenames.contains(file_asm.filename())
                        && let weaver_core::classify::FileRole::RarVolume {
                            volume_number,
                        } = file_asm.role()
                            && file_asm.is_complete() {
                                vols.push((
                                    *volume_number,
                                    state.working_dir.join(file_asm.filename()),
                                ));
                            }
                }
                vols.sort_by_key(|(vn, _)| *vn);
                let paths: Vec<PathBuf> = vols.into_iter().map(|(_, p)| p).collect();
                (paths, state.spec.password.clone(), state.working_dir.clone())
            };

            let output_dir = working_dir.clone();
            let event_tx = self.event_tx.clone();
            let members_to_extract = new_extractable.clone();

            let extract_result = tokio::task::spawn_blocking(move || {
                if volume_paths.is_empty() {
                    return Err("no complete RAR volumes".to_string());
                }

                let first_file = std::fs::File::open(&volume_paths[0])
                    .map_err(|e| format!("failed to open first volume: {e}"))?;
                let mut archive = if let Some(ref pw) = password {
                    weaver_rar::RarArchive::open_with_password(first_file, pw)
                } else {
                    weaver_rar::RarArchive::open(first_file)
                }
                .map_err(|e| format!("failed to open RAR archive: {e}"))?;

                for (i, path) in volume_paths.iter().enumerate().skip(1) {
                    let vol_file = std::fs::File::open(path)
                        .map_err(|e| format!("failed to open volume {i}: {e}"))?;
                    archive
                        .add_volume(i, Box::new(vol_file))
                        .map_err(|e| format!("failed to add volume {i}: {e}"))?;
                }

                let meta = archive.metadata();
                let options = weaver_rar::ExtractOptions {
                    verify: true,
                    password: password.clone(),
                };

                let mut extracted = Vec::new();
                for member_name in &members_to_extract {
                    let idx =
                        match meta.members.iter().position(|m| &m.name == member_name)
                        {
                            Some(i) => i,
                            None => continue,
                        };
                    let member = &meta.members[idx];
                    if member.is_directory {
                        let dir_path = output_dir.join(&member.name);
                        std::fs::create_dir_all(&dir_path).map_err(|e| {
                            format!("failed to create dir {}: {e}", member.name)
                        })?;
                        extracted.push(member_name.clone());
                        continue;
                    }

                    let out_path = output_dir.join(&member.name);
                    if let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| format!("failed to create parent dir: {e}"))?;
                    }
                    let bytes_written = archive
                        .extract_member_to_file(idx, &options, None, &out_path)
                        .map_err(|e| {
                            format!("failed to extract {}: {e}", member.name)
                        })?;

                    let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                        job_id,
                        member: member.name.clone(),
                        bytes_written,
                        total_bytes: member.unpacked_size.unwrap_or(0),
                    });

                    extracted.push(member_name.clone());
                }

                Ok(extracted)
            })
            .await;

            match extract_result {
                Ok(Ok(extracted)) => {
                    let already = self.extracted_members.get_mut(&job_id).unwrap();
                    for name in &extracted {
                        info!(job_id = job_id.0, set_name = %set_name, member = %name, "batch extraction: member extracted");
                        already.insert(name.clone());
                    }
                    info!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        extracted_count = already.len(),
                        "batch extraction progress"
                    );
                }
                Ok(Err(e)) => {
                    warn!(job_id = job_id.0, set_name = %set_name, error = %e, "batch extraction failed");
                }
                Err(e) => {
                    warn!(job_id = job_id.0, set_name = %set_name, error = %e, "batch extraction task panicked");
                }
            }
        }
    }
}
