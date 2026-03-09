use std::io::BufWriter;

use super::*;

impl Pipeline {
    /// Try partial extraction: extract archive members whose volumes are all present.
    /// Called after each file completes, not just when all files are done.
    pub(super) async fn try_partial_extraction(&mut self, job_id: JobId) {
        // Check for ready 7z sets — extract each independently.
        // RAR always goes through the streaming path below (no set readiness needed).
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
        let active_set_names: HashSet<String> = self
            .streaming_providers
            .keys()
            .filter(|(jid, _, _)| *jid == job_id)
            .map(|(_, sn, _)| sn.clone())
            .collect();
        for sn in &active_set_names {
            self.feed_streaming_volumes(job_id, sn);
        }

        // Try to start streaming extraction for additional members (up to concurrency limit).
        if self.try_start_streaming_extraction(job_id).await {
            return;
        }

        // Fall back to the original partial extraction path
        // (multi-member archives where some members are fully ready).
        self.try_batch_extraction(job_id).await;
    }

    /// Start streaming extraction for eligible members up to the concurrency limit.
    ///
    /// Returns true if any streaming extraction is active or was started,
    /// false if we should fall back to batch extraction.
    async fn try_start_streaming_extraction(&mut self, job_id: JobId) -> bool {
        // Count active streaming extractions for this job.
        let active_count = self
            .streaming_providers
            .keys()
            .filter(|(jid, _, _)| *jid == job_id)
            .count();

        let max_concurrent = self.tuner.max_concurrent_extractions();
        if active_count >= max_concurrent {
            return true;
        }
        let remaining_slots = max_concurrent - active_count;

        // Collect candidate members: first_volume complete, not extracted, not already streaming.
        let candidates: Vec<(String, String, u32)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return true;
            };
            match &state.status {
                JobStatus::Downloading | JobStatus::Verifying => {}
                _ => return true,
            }

            let mut found = Vec::new();
            for (sname, topo) in state.assembly.archive_topologies() {
                if topo.archive_type != weaver_assembly::ArchiveType::Rar {
                    continue;
                }
                for member in &topo.members {
                    if topo.complete_volumes.contains(&member.first_volume)
                        && !self
                            .extracted_members
                            .get(&job_id)
                            .is_some_and(|s| s.contains(&member.name))
                        && !self
                            .failed_extractions
                            .get(&job_id)
                            .is_some_and(|s| s.contains(&member.name))
                        && !self.streaming_providers.contains_key(&(
                            job_id,
                            sname.clone(),
                            member.name.clone(),
                        ))
                    {
                        found.push((sname.clone(), member.name.clone(), member.first_volume));
                    }
                }
            }
            found
        };

        if candidates.is_empty() {
            return active_count > 0;
        }

        let mut started = false;
        for (set_name, member_name, first_volume) in candidates.into_iter().take(remaining_slots) {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                member = %member_name,
                first_volume,
                "starting streaming extraction"
            );

            // Create the WaitingVolumeProvider and feed all currently-complete volumes.
            let provider = Arc::new(weaver_rar::WaitingVolumeProvider::new());
            let provider_key = (job_id, set_name.clone(), member_name.clone());
            self.streaming_providers
                .insert(provider_key.clone(), Arc::clone(&provider));
            self.streaming_volume_bases
                .insert(provider_key, first_volume);

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
                        && *volume_number == first_volume
                        && file_asm.is_complete()
                    {
                        path = Some(state.working_dir.join(file_asm.filename()));
                        break;
                    }
                }
                match path {
                    Some(p) => p,
                    None => {
                        warn!(job_id = job_id.0, set_name = %set_name, member = %member_name, "first volume path not found");
                        let key = (job_id, set_name.clone(), member_name.clone());
                        self.streaming_providers.remove(&key);
                        self.streaming_volume_bases.remove(&key);
                        continue;
                    }
                }
            };
            let password = state.spec.password.clone();
            let output_dir = state.working_dir.clone();
            let event_tx = self.event_tx.clone();
            let target_member = member_name.clone();

            // Spawn the blocking extraction task.
            let extraction_provider = Arc::clone(&provider);
            let extract_done_tx = self.extract_done_tx.clone();
            let set_name_for_done = set_name.clone();
            let member_for_done = member_name.clone();
            let set_name_for_task = set_name.clone();
            tokio::task::spawn(async move {
                let result = tokio::task::spawn_blocking(move || {
                    let first_file = std::fs::File::open(&first_vol_path)
                        .map_err(|e| format!("failed to open first volume: {e}"))?;
                    let mut archive = if let Some(ref pw) = password {
                        weaver_rar::RarArchive::open_with_password(first_file, pw)
                    } else {
                        weaver_rar::RarArchive::open(first_file)
                    }
                    .map_err(|e| format!("failed to open RAR archive: {e}"))?;

                    let options = weaver_rar::ExtractOptions {
                        verify: true,
                        password: password.clone(),
                    };

                    let idx = archive
                        .find_member_sanitized(&target_member)
                        .ok_or_else(|| format!("member {} not found in archive", target_member))?;

                    let member = archive
                        .member_info(idx)
                        .ok_or_else(|| format!("member index {} out of range", idx))?;
                    if member.is_directory {
                        let dir_path = output_dir.join(&member.name);
                        std::fs::create_dir_all(&dir_path)
                            .map_err(|e| format!("failed to create dir {}: {e}", member.name))?;
                        return Ok(vec![]);
                    }

                    let out_path = output_dir.join(&member.name);
                    if let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| format!("failed to create parent dir: {e}"))?;
                    }

                    // Sanitize set/member names for temp file naming.
                    let safe_set = set_name_for_task.replace('/', "__");
                    let safe_member = target_member.replace('/', "__");

                    // Chunked extraction: per-volume temp files.
                    let chunks = archive
                        .extract_member_streaming_chunked(
                            idx,
                            &options,
                            &*extraction_provider,
                            |vol_idx| {
                                let temp_path = output_dir.join(format!(
                                    ".chunk.{safe_set}.{safe_member}.vol{vol_idx:04}.tmp"
                                ));
                                let file = std::fs::File::create(&temp_path)
                                    .map_err(weaver_rar::RarError::Io)?;
                                Ok(Box::new(BufWriter::with_capacity(8 * 1024 * 1024, file))
                                    as Box<dyn std::io::Write>)
                            },
                        )
                        .map_err(|e| format!("streaming extraction failed: {e}"))?;

                    // Build chunk info with temp paths.
                    let chunk_info: Vec<(usize, u64, String)> = chunks
                        .iter()
                        .map(|&(vol_idx, bytes_written)| {
                            let temp_path = output_dir.join(format!(
                                ".chunk.{safe_set}.{safe_member}.vol{vol_idx:04}.tmp"
                            ));
                            (vol_idx, bytes_written, temp_path.display().to_string())
                        })
                        .collect();

                    // Concatenate temp files into final output.
                    let total_bytes: u64 = chunk_info.iter().map(|(_, b, _)| *b).sum();
                    {
                        let mut out = BufWriter::with_capacity(
                            8 * 1024 * 1024,
                            std::fs::File::create(&out_path)
                                .map_err(|e| format!("failed to create output file: {e}"))?,
                        );
                        for (_, _, temp_path) in &chunk_info {
                            let mut src = std::fs::File::open(temp_path)
                                .map_err(|e| format!("failed to open temp chunk: {e}"))?;
                            std::io::copy(&mut src, &mut out)
                                .map_err(|e| format!("failed to concatenate chunk: {e}"))?;
                        }
                        out.into_inner()
                            .map_err(|e| format!("failed to flush output: {e}"))?;
                    }

                    // Clean up temp files.
                    for (_, _, temp_path) in &chunk_info {
                        let _ = std::fs::remove_file(temp_path);
                    }

                    let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                        job_id,
                        member: member.name.clone(),
                        bytes_written: total_bytes,
                        total_bytes: member.unpacked_size.unwrap_or(0),
                    });

                    Ok(chunk_info)
                })
                .await;

                let result = match result {
                    Ok(Ok(chunks)) => Ok(chunks),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(format!("streaming extraction task panicked: {e}")),
                };
                let _ = extract_done_tx
                    .send(ExtractionDone::Streaming {
                        job_id,
                        set_name: set_name_for_done,
                        member: member_for_done,
                        result,
                    })
                    .await;
            });

            self.extracted_members
                .entry(job_id)
                .or_default()
                .insert(member_name);

            started = true;
        }

        started || active_count > 0
    }

    /// Feed all currently-complete volume paths for a RAR set to all active streaming providers.
    pub(super) fn feed_streaming_volumes(&self, job_id: JobId, set_name: &str) {
        // Find all providers for this (job_id, set_name) prefix.
        let matching_keys: Vec<(JobId, String, String)> = self
            .streaming_providers
            .keys()
            .filter(|(jid, sn, _)| *jid == job_id && sn == set_name)
            .cloned()
            .collect();

        if matching_keys.is_empty() {
            return;
        }

        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };

        let topo = match state.assembly.archive_topology_for(set_name) {
            Some(t) => t,
            None => return,
        };
        let set_filenames: HashSet<&str> = topo.volume_map.keys().map(|s| s.as_str()).collect();

        // Collect volume info once, then feed to each provider with its own base offset.
        let mut volume_info: Vec<(u32, PathBuf)> = Vec::new();
        for file_asm in state.assembly.files() {
            if set_filenames.contains(file_asm.filename())
                && let weaver_core::classify::FileRole::RarVolume { volume_number } =
                    file_asm.role()
                && file_asm.is_complete()
            {
                volume_info.push((*volume_number, state.working_dir.join(file_asm.filename())));
            }
        }

        for key in &matching_keys {
            let Some(provider) = self.streaming_providers.get(key) else {
                continue;
            };
            let base = self.streaming_volume_bases.get(key).copied().unwrap_or(0);
            let mut fed_count = 0u32;
            for (volume_number, path) in &volume_info {
                if *volume_number >= base {
                    let local_idx = (*volume_number - base) as usize;
                    provider.volume_ready(local_idx, path.clone());
                    fed_count += 1;
                }
            }
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                member = %key.2,
                fed_count,
                "fed streaming volumes"
            );

            // If the job is complete/failed, mark the provider as finished.
            match &state.status {
                JobStatus::Complete | JobStatus::Failed { .. } => {
                    provider.mark_finished();
                }
                _ => {}
            }
        }
    }

    /// Cancel streaming extraction for a job (called on job removal/failure).
    pub(super) fn cancel_streaming_extraction(&mut self, job_id: JobId) {
        let keys: Vec<(JobId, String, String)> = self
            .streaming_providers
            .keys()
            .filter(|(jid, _, _)| *jid == job_id)
            .cloned()
            .collect();
        for key in keys {
            if let Some(provider) = self.streaming_providers.remove(&key) {
                provider.mark_cancelled("job cancelled".into());
            }
            self.streaming_volume_bases.remove(&key);
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
                        && let weaver_core::classify::FileRole::RarVolume { volume_number } =
                            file_asm.role()
                        && file_asm.is_complete()
                    {
                        vols.push((*volume_number, state.working_dir.join(file_asm.filename())));
                    }
                }
                vols.sort_by_key(|(vn, _)| *vn);
                let paths: Vec<PathBuf> = vols.into_iter().map(|(_, p)| p).collect();
                (
                    paths,
                    state.spec.password.clone(),
                    state.working_dir.clone(),
                )
            };

            let output_dir = working_dir.clone();
            let event_tx = self.event_tx.clone();
            let members_to_extract = new_extractable.clone();

            // Pre-mark members as being extracted so we don't re-trigger.
            let already = self.extracted_members.entry(job_id).or_default();
            for name in &new_extractable {
                already.insert(name.clone());
            }

            let extract_done_tx = self.extract_done_tx.clone();
            let set_name_owned = set_name.clone();
            tokio::task::spawn(async move {
                let result = tokio::task::spawn_blocking(move || {
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
                        let idx = match meta.members.iter().position(|m| &m.name == member_name) {
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
                            .map_err(|e| format!("failed to extract {}: {e}", member.name))?;

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

                let extracted = match result {
                    Ok(r) => r,
                    Err(e) => Err(format!("extraction task panicked: {e}")),
                };
                let _ = extract_done_tx
                    .send(ExtractionDone::Batch {
                        job_id,
                        set_name: set_name_owned,
                        extracted,
                    })
                    .await;
            });
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

    fn mark_volume_range(
        target: &mut HashMap<(JobId, String), HashSet<u32>>,
        job_id: JobId,
        set_name: &str,
        first_volume: u32,
        last_volume: u32,
    ) {
        let entry = target.entry((job_id, set_name.to_string())).or_default();
        for volume in first_volume..=last_volume {
            entry.insert(volume);
        }
    }

    fn mark_member_volumes_clean(&mut self, job_id: JobId, set_name: &str, member_name: &str) {
        let Some((first_volume, last_volume)) =
            self.member_volume_span(job_id, set_name, member_name)
        else {
            return;
        };

        Self::mark_volume_range(
            &mut self.clean_volumes,
            job_id,
            set_name,
            first_volume,
            last_volume,
        );

        if let Some(suspect) = self
            .suspect_volumes
            .get_mut(&(job_id, set_name.to_string()))
        {
            for volume in first_volume..=last_volume {
                suspect.remove(&volume);
            }
            if suspect.is_empty() {
                self.suspect_volumes.remove(&(job_id, set_name.to_string()));
            }
        }
    }

    fn mark_member_volumes_suspect(&mut self, job_id: JobId, set_name: &str, member_name: &str) {
        let Some((first_volume, last_volume)) =
            self.member_volume_span(job_id, set_name, member_name)
        else {
            return;
        };

        let suspect_first = first_volume.saturating_sub(1);
        let suspect_last = last_volume.saturating_add(1);
        Self::mark_volume_range(
            &mut self.suspect_volumes,
            job_id,
            set_name,
            suspect_first,
            suspect_last,
        );
    }

    fn suspect_par2_file_ids_for_member(
        &self,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Vec<weaver_par2::FileId> {
        let Some(par2_set) = self.par2_sets.get(&job_id) else {
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
            let Some(par2_set) = self.par2_sets.get(&job_id).cloned() else {
                return;
            };
            (state.working_dir.clone(), par2_set)
        };

        let lower_bound = tokio::task::spawn_blocking(move || -> Result<u32, String> {
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

            let access = weaver_par2::PlacementFileAccess::from_plan(working_dir, &par2_set, &plan);
            let verification = weaver_par2::verify_selected_file_ids(&par2_set, &access, &file_ids);
            Ok(verification.total_missing_blocks)
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
                extracted,
            } => match extracted {
                Ok(names) => {
                    for name in &names {
                        info!(job_id = job_id.0, set_name = %set_name, member = %name, "batch extraction: member extracted");
                        self.mark_member_volumes_clean(job_id, &set_name, name);
                    }
                    self.try_delete_volumes(job_id, &set_name);
                    // If all archive members passed CRC, skip PAR2 entirely.
                    if !self.par2_bypassed.contains(&job_id) && self.all_members_crc_passed(job_id)
                    {
                        self.extracted_sets
                            .entry(job_id)
                            .or_default()
                            .insert(set_name.clone());
                        self.drain_recovery_and_bypass_par2(job_id).await;
                    }
                    // Members were pre-marked; check completion.
                    self.check_job_completion(job_id).await;
                }
                Err(e) => {
                    warn!(job_id = job_id.0, set_name = %set_name, error = %e, "batch extraction failed");
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state.status = JobStatus::Failed { error: e.clone() };
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::ExtractionFailed { job_id, error: e });
                        self.record_job_history(job_id);
                    }
                }
            },
            ExtractionDone::FullSet {
                job_id,
                set_name,
                result,
            } => match result {
                Ok((count, failed_members)) => {
                    if failed_members.is_empty() {
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            members = count,
                            "set extraction complete"
                        );
                        // Set was pre-marked in extracted_sets; confirm it.
                        self.extracted_sets
                            .entry(job_id)
                            .or_default()
                            .insert(set_name);
                    } else {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            succeeded = count,
                            failed = ?failed_members,
                            "set extraction completed with failures"
                        );
                        // Track failed members so they can be retried after repair.
                        for member in &failed_members {
                            if let Some(members) = self.extracted_members.get_mut(&job_id) {
                                members.remove(member);
                            }
                            self.failed_extractions
                                .entry(job_id)
                                .or_default()
                                .insert(member.clone());
                            self.mark_member_volumes_suspect(job_id, &set_name, member);
                            self.promote_recovery_for_failed_member(job_id, &set_name, member)
                                .await;
                        }
                        // Don't mark set as extracted — needs repair + retry.
                    }
                    self.check_job_completion(job_id).await;
                }
                Err(e) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %e,
                        "set extraction failed"
                    );
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
            ExtractionDone::Streaming {
                job_id,
                set_name,
                member,
                result,
            } => {
                // Clean up the streaming provider (must happen before
                // try_delete_volumes so the range is no longer "active").
                let key = (job_id, set_name.clone(), member.clone());
                self.streaming_volume_bases.remove(&key);
                self.streaming_providers.remove(&key);

                match result {
                    Ok(ref chunks) => {
                        let total_bytes: u64 = chunks.iter().map(|(_, b, _)| *b).sum();
                        info!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            member = %member,
                            bytes = total_bytes,
                            num_chunks = chunks.len(),
                            "streaming extraction complete"
                        );

                        self.mark_member_volumes_clean(job_id, &set_name, &member);

                        // Delete volumes no longer needed by any active stream.
                        self.try_delete_volumes(job_id, &set_name);

                        // If all archive members passed CRC, skip PAR2 entirely.
                        if !self.par2_bypassed.contains(&job_id)
                            && self.all_members_crc_passed(job_id)
                        {
                            // Mark the set as extracted so check_job_completion
                            // doesn't try to start a full-set extraction round.
                            self.extracted_sets
                                .entry(job_id)
                                .or_default()
                                .insert(set_name.clone());
                            self.drain_recovery_and_bypass_par2(job_id).await;
                        }
                    }
                    Err(e) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            member = %member,
                            error = %e,
                            "streaming extraction failed"
                        );

                        // Clean up orphan chunk files from the failed extraction.
                        if let Some(state) = self.jobs.get(&job_id) {
                            let safe_set = set_name.replace('/', "__");
                            let safe_member = member.replace('/', "__");
                            let prefix = format!(".chunk.{safe_set}.{safe_member}.vol");
                            if let Ok(entries) = std::fs::read_dir(&state.working_dir) {
                                for entry in entries.flatten() {
                                    let name = entry.file_name();
                                    let name_str = name.to_string_lossy();
                                    if name_str.starts_with(&prefix) && name_str.ends_with(".tmp") {
                                        let _ = std::fs::remove_file(entry.path());
                                    }
                                }
                            }
                        }

                        // Move from extracted_members to failed_extractions:
                        // - Removed from extracted_members so post-repair extraction can retry
                        // - Added to failed_extractions to prevent immediate retry during download
                        if let Some(members) = self.extracted_members.get_mut(&job_id) {
                            members.remove(&member);
                        }

                        self.failed_extractions
                            .entry(job_id)
                            .or_default()
                            .insert(member.clone());
                        self.mark_member_volumes_suspect(job_id, &set_name, &member);
                        self.promote_recovery_for_failed_member(job_id, &set_name, &member)
                            .await;
                    }
                }

                // If all downloads are complete and no streaming extractions remain,
                // transition to job completion (full-set extraction or finalization).
                // Otherwise, try to start more streaming extractions.
                let all_downloaded = self.jobs.get(&job_id).is_some_and(|s| {
                    s.assembly.complete_data_file_count() >= s.assembly.data_file_count()
                });
                let has_active_streaming = self
                    .streaming_providers
                    .keys()
                    .any(|(jid, _, _)| *jid == job_id);

                if all_downloaded && !has_active_streaming {
                    self.check_job_completion(job_id).await;
                } else {
                    self.try_partial_extraction(job_id).await;
                }
            }
        }
    }

    /// Check if all archive members for a job have been extracted with CRC pass.
    fn all_members_crc_passed(&self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        // If any extraction failed, PAR2 may be needed for repair.
        if self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|f| !f.is_empty())
        {
            return false;
        }
        let Some(extracted) = self.extracted_members.get(&job_id) else {
            return false;
        };
        // Check all archive topologies have all members extracted.
        let topos = state.assembly.archive_topologies();
        if topos.is_empty() {
            return false;
        }
        for (_, topo) in topos {
            for member in &topo.members {
                if !extracted.contains(&member.name) {
                    return false;
                }
            }
        }
        // No active streaming extractions remaining for this job.
        !self
            .streaming_providers
            .keys()
            .any(|(jid, _, _)| *jid == job_id)
    }

    /// All archive members passed CRC — drain recovery queue, delete PAR2 files,
    /// and mark verification as bypassed.
    async fn drain_recovery_and_bypass_par2(&mut self, job_id: JobId) {
        self.par2_bypassed.insert(job_id);

        // Drain recovery queue to stop downloading PAR2 recovery volumes.
        let drained = if let Some(state) = self.jobs.get_mut(&job_id) {
            state.recovery_queue.drain_all().len()
        } else {
            0
        };

        // Drop retained Par2FileSet (won't be needed for repair).
        self.par2_sets.remove(&job_id);
        self.promoted_recovery_files.remove(&job_id);

        // Delete PAR2 files from disk.
        let (par2_files, working_dir) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let files: Vec<String> = state
                .assembly
                .files()
                .filter(|f| matches!(f.role(), weaver_core::classify::FileRole::Par2 { .. }))
                .map(|f| f.filename().to_string())
                .collect();
            (files, state.working_dir.clone())
        };

        let mut deleted = 0u32;
        for filename in &par2_files {
            let path = working_dir.join(filename);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => deleted += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(file = %path.display(), error = %e, "failed to delete PAR2 file");
                }
            }
        }

        info!(
            job_id = job_id.0,
            drained, deleted, "all archive members CRC-verified — skipping PAR2"
        );
    }
}
