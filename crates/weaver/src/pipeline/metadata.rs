use super::*;
use crate::pipeline::rar_state::{self, RarDerivedPlan, RarSetPhase};
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
enum RarTopologyRebuildSource {
    CachedHeaders,
    VolumeZero,
}

impl RarTopologyRebuildSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::CachedHeaders => "cached-headers",
            Self::VolumeZero => "volume-0",
        }
    }
}

impl Pipeline {
    fn register_rar_volume_file(
        state: &mut crate::pipeline::rar_state::RarSetState,
        logical_volume: u32,
        filename: String,
    ) {
        state.volume_files.retain(|volume, existing_filename| {
            *existing_filename != filename || *volume == logical_volume
        });
        state.volume_files.insert(logical_volume, filename);
    }

    fn ownerless_rar_volumes(plan: &crate::pipeline::rar_state::RarDerivedPlan) -> Vec<u32> {
        plan.delete_decisions
            .iter()
            .filter_map(|(volume, decision)| decision.owners.is_empty().then_some(*volume))
            .collect()
    }

    async fn parse_rar_volume_facts_from_path(
        path: PathBuf,
        password: Option<String>,
    ) -> Result<weaver_rar::RarVolumeFacts, String> {
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path)
                .map_err(|e| format!("failed to open {}: {e}", path.display()))?;
            weaver_rar::RarArchive::parse_volume_facts(file, password.as_deref()).map_err(|e| {
                format!(
                    "failed to parse RAR volume facts from {}: {e}",
                    path.display()
                )
            })
        })
        .await
        .map_err(|e| format!("RAR facts parser task panicked: {e}"))?
    }

    pub(super) fn rar_volume_filename<'a>(
        volume_map: &'a HashMap<String, u32>,
        volume: u32,
    ) -> Option<&'a str> {
        volume_map.iter().find_map(|(filename, volume_number)| {
            (*volume_number == volume).then_some(filename.as_str())
        })
    }

    pub(super) fn is_rar_volume_deleted(
        &self,
        job_id: JobId,
        volume_map: &HashMap<String, u32>,
        volume: u32,
    ) -> bool {
        let Some(filename) = Self::rar_volume_filename(volume_map, volume) else {
            return false;
        };
        self.eagerly_deleted
            .get(&job_id)
            .is_some_and(|deleted| deleted.contains(filename))
    }

    pub(super) fn volume_paths_for_rar_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> std::collections::BTreeMap<u32, PathBuf> {
        let mut volume_paths = std::collections::BTreeMap::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return volume_paths;
        };

        if let Some(rar_state) = self.rar_sets.get(&(job_id, set_name.to_string())) {
            for (logical_volume, filename) in &rar_state.volume_files {
                let path = state.working_dir.join(filename);
                if path.exists() {
                    volume_paths.insert(*logical_volume, path);
                }
            }
        }

        if volume_paths.is_empty() {
            for file_asm in state.assembly.files() {
                let weaver_core::classify::FileRole::RarVolume { volume_number } = file_asm.role()
                else {
                    continue;
                };
                let base_name =
                    weaver_core::classify::archive_base_name(file_asm.filename(), file_asm.role());
                if base_name.as_deref() != Some(set_name) || !file_asm.is_complete() {
                    continue;
                }
                let path = state.working_dir.join(file_asm.filename());
                if path.exists() {
                    volume_paths.insert(*volume_number, path);
                }
            }
        }

        volume_paths
    }

    fn apply_rar_plan(&mut self, job_id: JobId, set_name: &str, plan: RarDerivedPlan) {
        let key = (job_id, set_name.to_string());
        let old_phase = self.rar_sets.get(&key).map(|state| state.phase);
        let old_eligible = self
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .map(|plan| plan.deletion_eligible.clone())
            .unwrap_or_default();
        let suspect_volumes = self.suspect_volumes.get(&key).cloned().unwrap_or_default();

        for volume in self
            .rar_sets
            .get(&key)
            .map(|state| state.facts.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default()
        {
            let eligible = plan.deletion_eligible.contains(&volume);
            let par2_clean = plan
                .delete_decisions
                .get(&volume)
                .is_some_and(crate::pipeline::Pipeline::claim_clean_rar_volume)
                && !suspect_volumes.contains(&volume);
            let deleted = self.is_rar_volume_deleted(job_id, &plan.topology.volume_map, volume);
            if let Err(error) = self
                .db
                .set_volume_status(job_id, set_name, volume, eligible, par2_clean, deleted)
            {
                error!(
                    job_id = job_id.0,
                    set_name,
                    volume,
                    error = %error,
                    "failed to persist RAR volume status"
                );
            }
        }

        if let Some(state) = self.jobs.get_mut(&job_id) {
            state
                .assembly
                .set_archive_topology(set_name.to_string(), plan.topology.clone());
        }

        if let Some(state) = self.rar_sets.get_mut(&key) {
            state.phase = plan.phase;
            state.plan = Some(plan.clone());
        }

        if old_phase != Some(plan.phase) || old_eligible != plan.deletion_eligible {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                phase = plan.phase.as_str(),
                solid = plan.is_solid,
                known_volumes = plan.topology.complete_volumes.len(),
                ready_members = plan.ready_members.len(),
                waiting_on = ?plan.waiting_on_volumes,
                ownership_eligible = ?plan.deletion_eligible,
                verified_suspect_volumes = ?suspect_volumes,
                "RAR set recomputed"
            );
        }

        for member in &plan.ready_members {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                member = %member.name,
                "RAR member ready"
            );
        }
        for volume in &plan.waiting_on_volumes {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                volume,
                "RAR set waiting on volume"
            );
        }
        for (volume, decision) in &plan.delete_decisions {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                volume,
                owners = ?decision.owners,
                clean_owners = ?decision.clean_owners,
                failed_owners = ?decision.failed_owners,
                pending_owners = ?decision.pending_owners,
                unresolved_boundary = decision.unresolved_boundary,
                ownership_eligible = decision.ownership_eligible,
                claim_clean = crate::pipeline::Pipeline::claim_clean_rar_volume(decision),
                verified_suspect = suspect_volumes.contains(volume),
                deleted = self.is_rar_volume_deleted(job_id, &plan.topology.volume_map, *volume),
                "RAR volume ownership audit"
            );
        }
        if let Some(reason) = &plan.fallback_reason {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                error = %reason,
                "RAR set entered fallback full-set mode"
            );
        }
    }

    pub(super) async fn recompute_rar_set_state(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), String> {
        let key = (job_id, set_name.to_string());
        let Some(existing) = self.rar_sets.get(&key).cloned() else {
            return Ok(());
        };
        let volume_map = self.build_rar_volume_map(job_id, set_name);
        if volume_map.is_empty() {
            return Ok(());
        }
        let volume_paths = self.volume_paths_for_rar_set(job_id, set_name);
        let password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.clone());
        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let failed = self
            .failed_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let facts = existing.facts.clone();
        let worker_active = existing.active_workers > 0;
        let cached_headers = self.load_rar_snapshot(job_id, set_name);
        let set_name_owned = set_name.to_string();

        let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let mut rebuild_source = if cached_headers.is_some() {
                RarTopologyRebuildSource::CachedHeaders
            } else {
                RarTopologyRebuildSource::VolumeZero
            };
            let mut archive = match cached_headers {
                Some(headers) => match weaver_rar::RarArchive::deserialize_headers_with_password(
                    &headers,
                    password.clone(),
                ) {
                    Ok(archive) => archive,
                    Err(error) => {
                        warn!(
                            set_name = %set_name_owned,
                            error = %error,
                            "failed to deserialize cached RAR headers during plan rebuild, falling back to volume 0"
                        );
                        rebuild_source = RarTopologyRebuildSource::VolumeZero;
                        let first_path = volume_paths.get(&0).ok_or_else(|| {
                            format!(
                                "RAR set '{set_name_owned}' cannot be rebuilt without cached headers or volume 0"
                            )
                        })?;
                        let first_file = std::fs::File::open(first_path).map_err(|e| {
                            format!(
                                "failed to open RAR volume 0 for set '{set_name_owned}': {e}"
                            )
                        })?;
                        if let Some(password) = password.as_deref() {
                            weaver_rar::RarArchive::open_with_password(first_file, password)
                        } else {
                            weaver_rar::RarArchive::open(first_file)
                        }
                        .map_err(|e| {
                            format!(
                                "failed to parse RAR volume 0 for set '{set_name_owned}': {e}"
                            )
                        })?
                    }
                },
                None => {
                    let first_path = volume_paths.get(&0).ok_or_else(|| {
                        format!("RAR set '{set_name_owned}' cannot be rebuilt without volume 0")
                    })?;
                    let first_file = std::fs::File::open(first_path).map_err(|e| {
                        format!("failed to open RAR volume 0 for set '{set_name_owned}': {e}")
                    })?;
                    if let Some(password) = password.as_deref() {
                        weaver_rar::RarArchive::open_with_password(first_file, password)
                    } else {
                        weaver_rar::RarArchive::open(first_file)
                    }
                    .map_err(|e| {
                        format!("failed to parse RAR volume 0 for set '{set_name_owned}': {e}")
                    })?
                }
            };

            for (volume_number, path) in &volume_paths {
                let volume_file = std::fs::File::open(path).map_err(|e| {
                    format!(
                        "failed to open RAR volume {volume_number} for set '{set_name_owned}': {e}"
                    )
                })?;
                if archive.has_volume(*volume_number as usize) {
                    archive.attach_volume_reader(*volume_number as usize, Box::new(volume_file));
                } else {
                    archive
                        .add_volume(*volume_number as usize, Box::new(volume_file))
                        .map_err(|e| {
                            format!(
                                "failed to integrate RAR volume {volume_number} for set '{set_name_owned}': {e}"
                            )
                        })?;
                }
            }

            let plan = rar_state::build_plan(
                volume_map,
                &facts,
                &archive,
                &extracted,
                &failed,
                worker_active,
            )?;
            Ok((plan, archive.serialize_headers(), rebuild_source))
        })
        .await
        .map_err(|e| format!("RAR plan task panicked: {e}"))?;

        match result {
            Ok((plan, headers, rebuild_source)) => {
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    rebuild_source = rebuild_source.as_str(),
                    "RAR plan rebuilt"
                );
                self.set_rar_snapshot(job_id, set_name, headers);
                self.apply_rar_plan(job_id, set_name, plan);
                Ok(())
            }
            Err(error) => {
                let mut fallback = existing.plan.clone().unwrap_or_else(|| RarDerivedPlan {
                    phase: RarSetPhase::FallbackFullSet,
                    is_solid: false,
                    ready_members: Vec::new(),
                    member_names: Vec::new(),
                    waiting_on_volumes: HashSet::new(),
                    deletion_eligible: HashSet::new(),
                    delete_decisions: BTreeMap::new(),
                    topology: weaver_assembly::ArchiveTopology {
                        archive_type: weaver_assembly::ArchiveType::Rar,
                        volume_map: self.build_rar_volume_map(job_id, set_name),
                        complete_volumes: existing.facts.keys().copied().collect(),
                        expected_volume_count: None,
                        members: Vec::new(),
                        unresolved_spans: Vec::new(),
                    },
                    fallback_reason: Some(error.clone()),
                });
                fallback.phase = RarSetPhase::FallbackFullSet;
                fallback.fallback_reason = Some(error.clone());
                self.apply_rar_plan(job_id, set_name, fallback);
                Err(error)
            }
        }
    }

    pub(super) async fn refresh_rar_volume_facts_for_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
        touched_filenames: &HashSet<String>,
    ) -> Result<(), String> {
        let volume_paths: Vec<(u32, String, PathBuf)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };

            state
                .assembly
                .files()
                .filter_map(|file_asm| {
                    if !touched_filenames.contains(file_asm.filename()) {
                        return None;
                    }
                    let weaver_core::classify::FileRole::RarVolume { volume_number } =
                        file_asm.role()
                    else {
                        return None;
                    };
                    let base_name = weaver_core::classify::archive_base_name(
                        file_asm.filename(),
                        file_asm.role(),
                    );
                    if base_name.as_deref() != Some(set_name) || !file_asm.is_complete() {
                        return None;
                    }
                    let path = state.working_dir.join(file_asm.filename());
                    path.exists()
                        .then_some((*volume_number, file_asm.filename().to_string(), path))
                })
                .collect()
        };
        if volume_paths.is_empty() {
            return Ok(());
        }

        let password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.clone());
        let mut parsed = Vec::new();
        for (expected_volume_number, filename, path) in volume_paths {
            let facts = Self::parse_rar_volume_facts_from_path(path, password.clone()).await?;
            if facts.volume_number != expected_volume_number {
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    filename = %filename,
                    expected_volume = expected_volume_number,
                    parsed_volume = facts.volume_number,
                    "RAR facts refresh detected volume-number mismatch after normalization"
                );
            }
            parsed.push((facts.volume_number, filename, facts));
        }

        let state = self
            .rar_sets
            .entry((job_id, set_name.to_string()))
            .or_default();
        state
            .volume_files
            .retain(|_, existing_filename| !touched_filenames.contains(existing_filename));
        for (volume_number, filename, facts) in parsed {
            let encoded = rmp_serde::to_vec_named(&facts)
                .map_err(|e| format!("failed to encode RAR facts: {e}"))?;
            if let Err(error) =
                self.db
                    .save_rar_volume_facts(job_id, set_name, volume_number, &encoded)
            {
                return Err(format!("failed to persist RAR facts: {error}"));
            }
            Self::register_rar_volume_file(state, volume_number, filename);
            state.facts.insert(volume_number, facts);
        }
        self.recompute_rar_set_state(job_id, set_name).await
    }

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

    fn set_rar_snapshot(&mut self, job_id: JobId, set_name: &str, headers: Vec<u8>) {
        self.rar_header_snapshots
            .insert((job_id, set_name.to_string()), headers.clone());
        if let Err(e) = self.db.save_archive_headers(job_id, set_name, &headers) {
            error!(job_id = job_id.0, set_name, error = %e, "failed to persist RAR headers");
        }
    }

    pub(super) fn load_rar_snapshot(&self, job_id: JobId, set_name: &str) -> Option<Vec<u8>> {
        self.rar_header_snapshots
            .get(&(job_id, set_name.to_string()))
            .cloned()
            .or_else(|| {
                self.db
                    .load_archive_headers(job_id, set_name)
                    .ok()
                    .flatten()
            })
    }

    pub(super) fn clear_rar_snapshot(&mut self, job_id: JobId, set_name: &str) {
        self.rar_header_snapshots
            .remove(&(job_id, set_name.to_string()));
        if let Err(e) = self.db.delete_archive_headers(job_id, set_name) {
            error!(job_id = job_id.0, set_name, error = %e, "failed to delete cached RAR headers");
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.assembly.archive_topologies_mut().remove(set_name);
        }
    }

    fn build_rar_volume_map(&self, job_id: JobId, set_name: &str) -> HashMap<String, u32> {
        let Some(state) = self.jobs.get(&job_id) else {
            return HashMap::new();
        };
        if let Some(rar_state) = self.rar_sets.get(&(job_id, set_name.to_string())) {
            let volume_map = rar_state
                .volume_files
                .iter()
                .map(|(logical_volume, filename)| (filename.clone(), *logical_volume))
                .collect::<HashMap<_, _>>();
            if !volume_map.is_empty() {
                return volume_map;
            }
        }
        state
            .assembly
            .files()
            .filter_map(|file_asm| match file_asm.role() {
                weaver_core::classify::FileRole::RarVolume { volume_number } => {
                    let base_name = weaver_core::classify::archive_base_name(
                        file_asm.filename(),
                        file_asm.role(),
                    );
                    if base_name.as_deref() == Some(set_name) && file_asm.is_complete() {
                        Some((file_asm.filename().to_string(), *volume_number))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect::<HashMap<_, _>>()
    }

    pub(super) async fn restore_rar_state_for_job(&mut self, job_id: JobId) {
        let password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.clone());
        match self.db.load_all_archive_headers(job_id) {
            Ok(headers_by_set) => {
                for (set_name, headers) in headers_by_set {
                    match weaver_rar::RarArchive::deserialize_headers_with_password(
                        &headers,
                        password.clone(),
                    ) {
                        Ok(_) => {
                            self.rar_header_snapshots
                                .insert((job_id, set_name), headers);
                        }
                        Err(error) => {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                error = %error,
                                "dropping invalid persisted RAR header snapshot"
                            );
                            self.clear_rar_snapshot(job_id, &set_name);
                        }
                    }
                }
            }
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted RAR header snapshots"
                );
            }
        }

        let facts_by_set = match self.db.load_all_rar_volume_facts(job_id) {
            Ok(facts) => facts,
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted RAR volume facts"
                );
                return;
            }
        };

        for (set_name, facts_rows) in facts_by_set {
            let state = self.rar_sets.entry((job_id, set_name.clone())).or_default();
            state.facts.clear();
            state.volume_files.clear();
            for (volume_index, blob) in facts_rows {
                match rmp_serde::from_slice::<weaver_rar::RarVolumeFacts>(&blob) {
                    Ok(facts) => {
                        if facts.volume_number != volume_index {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                stored_volume = volume_index,
                                parsed_volume = facts.volume_number,
                                "persisted RAR facts volume key did not match parsed volume number"
                            );
                        }
                        state.facts.insert(facts.volume_number, facts);
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume_index,
                            error = %error,
                            "dropping invalid persisted RAR volume facts"
                        );
                    }
                }
            }
        }

        let set_names: Vec<String> = self
            .rar_sets
            .keys()
            .filter_map(|(jid, set_name)| (*jid == job_id).then_some(set_name.clone()))
            .collect();

        for set_name in &set_names {
            let volume_files = {
                let Some(state) = self.jobs.get(&job_id) else {
                    return;
                };
                state
                    .assembly
                    .files()
                    .filter_map(|file_asm| {
                        let weaver_core::classify::FileRole::RarVolume { .. } = file_asm.role()
                        else {
                            return None;
                        };
                        let base_name = weaver_core::classify::archive_base_name(
                            file_asm.filename(),
                            file_asm.role(),
                        );
                        if base_name.as_deref() != Some(set_name.as_str())
                            || !file_asm.is_complete()
                        {
                            return None;
                        }
                        let path = state.working_dir.join(file_asm.filename());
                        path.exists()
                            .then_some((file_asm.filename().to_string(), path))
                    })
                    .collect::<Vec<_>>()
            };
            for (filename, path) in volume_files {
                match Self::parse_rar_volume_facts_from_path(path, password.clone()).await {
                    Ok(facts) => {
                        if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                            Self::register_rar_volume_file(state, facts.volume_number, filename);
                            state.facts.entry(facts.volume_number).or_insert(facts);
                        }
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            filename = %filename,
                            error,
                            "failed to rebuild live RAR volume-file map during restore"
                        );
                    }
                }
            }
            let _ = self.recompute_rar_set_state(job_id, set_name).await;
        }

        match self.db.load_deleted_volume_statuses(job_id) {
            Ok(deleted_rows) => {
                for (set_name, volume_index) in deleted_rows {
                    let volume_map = self.build_rar_volume_map(job_id, &set_name);
                    if let Some(filename) = Self::rar_volume_filename(&volume_map, volume_index) {
                        self.eagerly_deleted
                            .entry(job_id)
                            .or_default()
                            .insert(filename.to_string());
                    }
                }
            }
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load deleted RAR volume status"
                );
            }
        }

        for set_name in set_names {
            let ownerless_volumes = self
                .rar_sets
                .get(&(job_id, set_name.clone()))
                .and_then(|state| state.plan.as_ref())
                .map(Self::ownerless_rar_volumes)
                .unwrap_or_default();
            if !ownerless_volumes.is_empty() {
                error!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volumes = ?ownerless_volumes,
                    "skipping restore-time eager delete because restored RAR plan has ownerless volumes"
                );
                continue;
            }
            self.try_delete_volumes(job_id, &set_name);
        }
    }

    /// When a RAR volume file completes, rebuild topology from the persistent
    /// multi-volume header snapshot instead of inferring spans by volume order.
    pub(super) async fn try_update_archive_topology(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (volume_number, filename, path, password) = {
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
                state.working_dir.join(file_asm.filename()),
                state.spec.password.clone(),
            )
        };

        let set_name = weaver_core::classify::archive_base_name(
            &filename,
            &weaver_core::classify::FileRole::RarVolume { volume_number },
        )
        .unwrap_or_else(|| "rar".into());

        match Self::parse_rar_volume_facts_from_path(path, password).await {
            Ok(facts) => {
                if facts.volume_number != volume_number {
                    info!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        filename = %filename,
                        expected_volume = volume_number,
                        parsed_volume = facts.volume_number,
                        "RAR facts parse detected filename-to-volume mismatch"
                    );
                }
                let encoded = match rmp_serde::to_vec_named(&facts) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            volume = volume_number,
                            set_name = %set_name,
                            error = %error,
                            "failed to encode RAR volume facts"
                        );
                        return;
                    }
                };
                if let Err(error) =
                    self.db
                        .save_rar_volume_facts(job_id, &set_name, facts.volume_number, &encoded)
                {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_number,
                        set_name = %set_name,
                        error = %error,
                        "failed to persist RAR volume facts"
                    );
                    return;
                }

                let state = self.rar_sets.entry((job_id, set_name.clone())).or_default();
                Self::register_rar_volume_file(state, facts.volume_number, filename.clone());
                state.facts.insert(facts.volume_number, facts);
                debug!(
                    job_id = job_id.0,
                    volume = volume_number,
                    set_name = %set_name,
                    "RAR volume facts parsed"
                );
                if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_number,
                        set_name = %set_name,
                        error,
                        "failed to recompute RAR set state"
                    );
                }
            }
            Err(error) => warn!(
                job_id = job_id.0,
                volume = volume_number,
                set_name = %set_name,
                error,
                "failed to parse RAR volume facts"
            ),
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
                    unresolved_spans: vec![],
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
                    unresolved_spans: vec![],
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
