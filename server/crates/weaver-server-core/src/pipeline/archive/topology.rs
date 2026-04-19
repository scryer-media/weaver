use super::rar_state::{self, RarDerivedPlan, RarSetPhase, RarSetState};
use crate::jobs::assembly::{
    ArchiveMember as JobArchiveMember, ArchivePendingSpan as JobArchivePendingSpan,
    ArchiveTopology as JobArchiveTopology, ArchiveType,
};
use crate::jobs::ids::{JobId, NzbFileId};
use crate::pipeline::Pipeline;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use tracing::{debug, error, info, warn};

pub(crate) type ArchiveMember = JobArchiveMember;
pub(crate) type ArchivePendingSpan = JobArchivePendingSpan;
pub(crate) type ArchiveTopology = JobArchiveTopology;

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
    fn register_rar_volume_file(state: &mut RarSetState, logical_volume: u32, filename: String) {
        state.volume_files.retain(|volume, existing_filename| {
            *existing_filename != filename || *volume == logical_volume
        });
        state.volume_files.insert(logical_volume, filename);
    }

    fn ownerless_rar_volumes(plan: &RarDerivedPlan) -> Vec<u32> {
        plan.delete_decisions
            .iter()
            .filter_map(|(volume, decision)| decision.owners.is_empty().then_some(*volume))
            .collect()
    }

    pub(in crate::pipeline::archive) async fn parse_rar_volume_facts_from_path(
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

    pub(in crate::pipeline::archive) fn persist_rar_volume_facts(
        &mut self,
        job_id: JobId,
        set_name: &str,
        filename: &str,
        observed_volume: Option<u32>,
        facts: weaver_rar::RarVolumeFacts,
    ) -> Result<bool, String> {
        if let Some(expected_volume) = observed_volume
            && facts.volume_number != expected_volume
        {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                filename = %filename,
                expected_volume,
                parsed_volume = facts.volume_number,
                "RAR facts parse detected filename-to-volume mismatch"
            );
        }

        let state_key = (job_id, set_name.to_string());
        let unchanged = self.rar_sets.get(&state_key).is_some_and(|state| {
            state.facts.get(&facts.volume_number) == Some(&facts)
                && state.volume_files.get(&facts.volume_number) == Some(&filename.to_string())
        });
        if unchanged {
            return Ok(false);
        }

        let encoded = rmp_serde::to_vec_named(&facts)
            .map_err(|error| format!("failed to encode RAR volume facts: {error}"))?;
        self.db
            .save_rar_volume_facts(job_id, set_name, facts.volume_number, &encoded)
            .map_err(|error| format!("failed to persist RAR volume facts: {error}"))?;

        let state = self.rar_sets.entry(state_key).or_default();
        Self::register_rar_volume_file(state, facts.volume_number, filename.to_string());
        state.facts.insert(facts.volume_number, facts);

        Ok(true)
    }

    pub(in crate::pipeline) fn rar_volume_filename(
        volume_map: &HashMap<String, u32>,
        volume: u32,
    ) -> Option<&str> {
        volume_map.iter().find_map(|(filename, volume_number)| {
            (*volume_number == volume).then_some(filename.as_str())
        })
    }

    pub(in crate::pipeline) fn is_rar_volume_deleted(
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

    pub(in crate::pipeline) fn volume_paths_for_rar_set(
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
                if let Some(path) = self.resolve_job_input_path(job_id, filename) {
                    if path.exists() {
                        volume_paths.insert(*logical_volume, path);
                    }
                }
            }
        }

        for file_asm in state.assembly.files() {
            let weaver_model::files::FileRole::RarVolume { volume_number } = file_asm.role() else {
                continue;
            };
            let base_name =
                weaver_model::files::archive_base_name(file_asm.filename(), file_asm.role());
            if base_name.as_deref() != Some(set_name) || !file_asm.is_complete() {
                continue;
            }
            if let Some(path) = self.resolve_job_input_path(job_id, file_asm.filename()) {
                if path.exists() {
                    volume_paths.entry(*volume_number).or_insert(path);
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
        let verified_suspect_volumes = self
            .rar_sets
            .get(&key)
            .map(|state| state.verified_suspect_volumes.clone())
            .unwrap_or_default();

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
                .is_some_and(Self::claim_clean_rar_volume)
                && !verified_suspect_volumes.contains(&volume);
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
                verified_suspect_volumes = ?verified_suspect_volumes,
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
                claim_clean = Self::claim_clean_rar_volume(decision),
                verified_suspect = verified_suspect_volumes.contains(volume),
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

    pub(in crate::pipeline) async fn recompute_rar_set_state(
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
                    topology: ArchiveTopology {
                        archive_type: ArchiveType::Rar,
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

    pub(in crate::pipeline) async fn refresh_rar_volume_facts_for_set(
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
                    let weaver_model::files::FileRole::RarVolume { volume_number } =
                        file_asm.role()
                    else {
                        return None;
                    };
                    let base_name = weaver_model::files::archive_base_name(
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

    fn set_rar_snapshot(&mut self, job_id: JobId, set_name: &str, headers: Vec<u8>) {
        self.rar_sets
            .entry((job_id, set_name.to_string()))
            .or_default()
            .cached_headers = Some(headers.clone());
        if let Err(e) = self.db.save_archive_headers(job_id, set_name, &headers) {
            error!(job_id = job_id.0, set_name, error = %e, "failed to persist RAR headers");
        }
    }

    pub(in crate::pipeline) fn load_rar_snapshot(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Option<Vec<u8>> {
        self.rar_sets
            .get(&(job_id, set_name.to_string()))
            .and_then(|state| state.cached_headers.clone())
            .or_else(|| {
                self.db
                    .load_archive_headers(job_id, set_name)
                    .ok()
                    .flatten()
            })
    }

    pub(in crate::pipeline) fn clear_rar_snapshot(&mut self, job_id: JobId, set_name: &str) {
        if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.to_string())) {
            state.cached_headers = None;
        }
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
        let mut volume_map = HashMap::new();
        if let Some(rar_state) = self.rar_sets.get(&(job_id, set_name.to_string())) {
            for (logical_volume, filename) in &rar_state.volume_files {
                volume_map.insert(filename.clone(), *logical_volume);
            }
        }
        for file_asm in state.assembly.files() {
            let weaver_model::files::FileRole::RarVolume { volume_number } = file_asm.role() else {
                continue;
            };
            let base_name =
                weaver_model::files::archive_base_name(file_asm.filename(), file_asm.role());
            if base_name.as_deref() == Some(set_name) && file_asm.is_complete() {
                volume_map
                    .entry(file_asm.filename().to_string())
                    .or_insert(*volume_number);
            }
        }
        volume_map
    }

    pub(crate) async fn restore_rar_state_for_job(&mut self, job_id: JobId) {
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
                            self.rar_sets
                                .entry((job_id, set_name))
                                .or_default()
                                .cached_headers = Some(headers);
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

        match self.db.load_verified_suspect_volumes(job_id) {
            Ok(verified_suspect_by_set) => {
                for (set_name, verified_suspect_volumes) in verified_suspect_by_set {
                    self.rar_sets
                        .entry((job_id, set_name))
                        .or_default()
                        .verified_suspect_volumes = verified_suspect_volumes;
                }
            }
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted verified suspect RAR volumes"
                );
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
                        let weaver_model::files::FileRole::RarVolume { .. } = file_asm.role()
                        else {
                            return None;
                        };
                        let base_name = weaver_model::files::archive_base_name(
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
    pub(crate) async fn try_update_archive_topology(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (volume_number, filename, path, password) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };
            let vol = match file_asm.role() {
                weaver_model::files::FileRole::RarVolume { volume_number } => *volume_number,
                _ => return,
            };
            (
                vol,
                file_asm.filename().to_string(),
                state.working_dir.join(file_asm.filename()),
                state.spec.password.clone(),
            )
        };

        let set_name = weaver_model::files::archive_base_name(
            &filename,
            &weaver_model::files::FileRole::RarVolume { volume_number },
        )
        .unwrap_or_else(|| "rar".into());

        match Self::parse_rar_volume_facts_from_path(path, password).await {
            Ok(facts) => {
                if let Err(error) = self.persist_rar_volume_facts(
                    job_id,
                    &set_name,
                    &filename,
                    Some(volume_number),
                    facts,
                ) {
                    warn!(
                        job_id = job_id.0,
                        volume = volume_number,
                        set_name = %set_name,
                        error = %error,
                        "failed to register RAR volume facts"
                    );
                    return;
                }
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

    /// When a non-RAR archive file completes, build or update the archive topology.
    ///
    /// Handles 7z, zip, tar, tar.gz, gz, and plain split files.
    /// Groups files by base name so that multiple independent archive sets
    /// within a single job are tracked separately.
    pub(crate) fn try_update_7z_topology(&mut self, job_id: JobId, file_id: NzbFileId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            return;
        };

        let role = file_asm.role().clone();
        let filename = file_asm.filename().to_string();
        let set_name = match weaver_model::files::archive_base_name(&filename, &role) {
            Some(name) => name,
            None => return,
        };

        match role {
            weaver_model::files::FileRole::SevenZipArchive => {
                if state.assembly.archive_topology_for(&set_name).is_some() {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.assembly.mark_volume_complete(&set_name, 0);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        "7z single-file volume complete"
                    );
                    return;
                }

                let mut volume_map = std::collections::HashMap::new();
                volume_map.insert(filename.clone(), 0);

                let topology = ArchiveTopology {
                    archive_type: ArchiveType::SevenZip,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(1),
                    members: vec![ArchiveMember {
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
            weaver_model::files::FileRole::SevenZipSplit { number } => {
                let completing_number = number;

                if state.assembly.archive_topology_for(&set_name).is_some() {
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

                let mut volume_map = std::collections::HashMap::new();
                let mut max_number = 0u32;
                for f in state.assembly.files() {
                    if let weaver_model::files::FileRole::SevenZipSplit { number: n } = f.role() {
                        let f_base = weaver_model::files::archive_base_name(f.filename(), f.role());
                        if f_base.as_deref() == Some(&set_name) {
                            volume_map.insert(f.filename().to_string(), *n);
                            max_number = max_number.max(*n);
                        }
                    }
                }

                let expected = max_number + 1;
                let topology = ArchiveTopology {
                    archive_type: ArchiveType::SevenZip,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(expected),
                    members: vec![ArchiveMember {
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

                let completed: Vec<u32> = state
                    .assembly
                    .files()
                    .filter(|f| f.is_complete())
                    .filter_map(|f| {
                        if let weaver_model::files::FileRole::SevenZipSplit { number: n } = f.role()
                        {
                            let f_base =
                                weaver_model::files::archive_base_name(f.filename(), f.role());
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
            weaver_model::files::FileRole::ZipArchive
            | weaver_model::files::FileRole::TarArchive
            | weaver_model::files::FileRole::TarGzArchive
            | weaver_model::files::FileRole::TarBz2Archive
            | weaver_model::files::FileRole::GzArchive
            | weaver_model::files::FileRole::DeflateArchive
            | weaver_model::files::FileRole::BrotliArchive
            | weaver_model::files::FileRole::ZstdArchive
            | weaver_model::files::FileRole::Bzip2Archive => {
                if state.assembly.archive_topology_for(&set_name).is_some() {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.assembly.mark_volume_complete(&set_name, 0);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        "single-file archive complete"
                    );
                    return;
                }

                let archive_type = match role {
                    weaver_model::files::FileRole::ZipArchive => ArchiveType::Zip,
                    weaver_model::files::FileRole::TarArchive => ArchiveType::Tar,
                    weaver_model::files::FileRole::TarGzArchive => ArchiveType::TarGz,
                    weaver_model::files::FileRole::TarBz2Archive => ArchiveType::TarBz2,
                    weaver_model::files::FileRole::GzArchive => ArchiveType::Gz,
                    weaver_model::files::FileRole::DeflateArchive => ArchiveType::Deflate,
                    weaver_model::files::FileRole::BrotliArchive => ArchiveType::Brotli,
                    weaver_model::files::FileRole::ZstdArchive => ArchiveType::Zstd,
                    weaver_model::files::FileRole::Bzip2Archive => ArchiveType::Bzip2,
                    _ => unreachable!(),
                };

                let mut volume_map = std::collections::HashMap::new();
                volume_map.insert(filename.clone(), 0);

                let topology = ArchiveTopology {
                    archive_type,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(1),
                    members: vec![ArchiveMember {
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
                    "single-file archive topology set"
                );
            }
            weaver_model::files::FileRole::SplitFile { number } => {
                let completing_number = number;

                if state.assembly.archive_topology_for(&set_name).is_some() {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state
                        .assembly
                        .mark_volume_complete(&set_name, completing_number);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        volume = completing_number,
                        "split file volume complete"
                    );
                    return;
                }

                let mut volume_map = std::collections::HashMap::new();
                let mut max_number = 0u32;
                for f in state.assembly.files() {
                    if let weaver_model::files::FileRole::SplitFile { number: n } = f.role() {
                        let f_base = weaver_model::files::archive_base_name(f.filename(), f.role());
                        if f_base.as_deref() == Some(&set_name) {
                            volume_map.insert(f.filename().to_string(), *n);
                            max_number = max_number.max(*n);
                        }
                    }
                }

                let expected = max_number + 1;
                let topology = ArchiveTopology {
                    archive_type: ArchiveType::Split,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(expected),
                    members: vec![ArchiveMember {
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

                let completed: Vec<u32> = state
                    .assembly
                    .files()
                    .filter(|f| f.is_complete())
                    .filter_map(|f| {
                        if let weaver_model::files::FileRole::SplitFile { number: n } = f.role() {
                            let f_base =
                                weaver_model::files::archive_base_name(f.filename(), f.role());
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
                    "split file topology set"
                );
            }
            _ => {}
        }
    }
}
