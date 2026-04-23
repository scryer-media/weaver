use super::*;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

impl Pipeline {
    fn nested_declared_archive_identity(
        filename: &str,
        role: &weaver_model::files::FileRole,
    ) -> Option<crate::jobs::assembly::DetectedArchiveIdentity> {
        let set_name = weaver_model::files::archive_base_name(filename, role)?;
        match role {
            weaver_model::files::FileRole::RarVolume { volume_number } => {
                Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name,
                    volume_index: Some(*volume_number),
                })
            }
            weaver_model::files::FileRole::SevenZipArchive => {
                Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSingle,
                    set_name,
                    volume_index: None,
                })
            }
            weaver_model::files::FileRole::SevenZipSplit { number } => {
                Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSplit,
                    set_name,
                    volume_index: Some(*number),
                })
            }
            _ => None,
        }
    }

    fn nested_scan_root(&self, job_id: JobId) -> Result<PathBuf, String> {
        let state = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| format!("job {} not found", job_id.0))?;
        Ok(state
            .staging_dir
            .clone()
            .unwrap_or_else(|| state.working_dir.clone()))
    }

    fn archive_type_for_role(
        role: &weaver_model::files::FileRole,
    ) -> Option<crate::jobs::assembly::ArchiveType> {
        match role {
            weaver_model::files::FileRole::RarVolume { .. } => {
                Some(crate::jobs::assembly::ArchiveType::Rar)
            }
            weaver_model::files::FileRole::SevenZipArchive
            | weaver_model::files::FileRole::SevenZipSplit { .. } => {
                Some(crate::jobs::assembly::ArchiveType::SevenZip)
            }
            weaver_model::files::FileRole::ZipArchive => {
                Some(crate::jobs::assembly::ArchiveType::Zip)
            }
            weaver_model::files::FileRole::TarArchive => {
                Some(crate::jobs::assembly::ArchiveType::Tar)
            }
            weaver_model::files::FileRole::TarGzArchive => {
                Some(crate::jobs::assembly::ArchiveType::TarGz)
            }
            weaver_model::files::FileRole::TarBz2Archive => {
                Some(crate::jobs::assembly::ArchiveType::TarBz2)
            }
            weaver_model::files::FileRole::GzArchive => {
                Some(crate::jobs::assembly::ArchiveType::Gz)
            }
            weaver_model::files::FileRole::DeflateArchive => {
                Some(crate::jobs::assembly::ArchiveType::Deflate)
            }
            weaver_model::files::FileRole::BrotliArchive => {
                Some(crate::jobs::assembly::ArchiveType::Brotli)
            }
            weaver_model::files::FileRole::ZstdArchive => {
                Some(crate::jobs::assembly::ArchiveType::Zstd)
            }
            weaver_model::files::FileRole::Bzip2Archive => {
                Some(crate::jobs::assembly::ArchiveType::Bzip2)
            }
            weaver_model::files::FileRole::SplitFile { .. } => {
                Some(crate::jobs::assembly::ArchiveType::Split)
            }
            _ => None,
        }
    }

    fn archive_volume_number(role: &weaver_model::files::FileRole) -> u32 {
        match role {
            weaver_model::files::FileRole::RarVolume { volume_number } => *volume_number,
            weaver_model::files::FileRole::SevenZipSplit { number }
            | weaver_model::files::FileRole::SplitFile { number } => *number,
            _ => 0,
        }
    }

    fn synthetic_segment_sizes(size: u64) -> Vec<u32> {
        if size == 0 {
            return vec![0];
        }

        let mut remaining = size;
        let mut segments = Vec::new();
        while remaining > 0 {
            let next = remaining.min(u32::MAX as u64) as u32;
            segments.push(next);
            remaining -= u64::from(next);
        }
        segments
    }

    async fn detect_nested_archive_identities(
        root: &Path,
        files: &mut [ScannedExtractionFile],
        password: Option<String>,
    ) -> Result<(), String> {
        let mut numeric_groups = HashMap::<String, Vec<(usize, u32)>>::new();
        for (index, file) in files.iter().enumerate() {
            if let Some((set_key, Some(numeric_suffix))) =
                crate::pipeline::archive::probe::probe_set_key_and_suffix(
                    &file.relative_path,
                    &file.declared_role,
                )
            {
                numeric_groups
                    .entry(set_key)
                    .or_default()
                    .push((index, numeric_suffix));
            }
        }

        let mut detected_split_7z_sets = HashSet::new();
        for file in files.iter_mut() {
            if file.detected_archive.is_some() {
                continue;
            }
            if !matches!(
                file.declared_role,
                weaver_model::files::FileRole::Unknown
                    | weaver_model::files::FileRole::SplitFile { .. }
            ) {
                continue;
            }

            let Some((set_key, numeric_suffix)) =
                crate::pipeline::archive::probe::probe_set_key_and_suffix(
                    &file.relative_path,
                    &file.declared_role,
                )
            else {
                continue;
            };

            let path = root.join(&file.relative_path);
            if !path.exists() {
                continue;
            }
            if let Ok(facts) =
                Pipeline::parse_rar_volume_facts_from_path(path.clone(), password.clone()).await
            {
                file.detected_archive = Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: set_key,
                    volume_index: Some(facts.volume_number),
                });
                continue;
            }

            if numeric_suffix.is_some() {
                if Self::path_has_7z_signature(path).await.unwrap_or(false) {
                    detected_split_7z_sets.insert(set_key);
                }
                continue;
            }

            if Self::path_has_7z_signature(path).await.unwrap_or(false) {
                file.detected_archive = Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSingle,
                    set_name: set_key,
                    volume_index: None,
                });
            }
        }

        for set_key in detected_split_7z_sets {
            let Some(group) = numeric_groups.get(&set_key) else {
                continue;
            };
            let mut ordered_group = group.clone();
            ordered_group.sort_by_key(|(_, numeric_suffix)| *numeric_suffix);
            for (normalized_index, (file_index, _)) in ordered_group.into_iter().enumerate() {
                if files[file_index].detected_archive.is_some() {
                    continue;
                }
                files[file_index].detected_archive =
                    Some(crate::jobs::assembly::DetectedArchiveIdentity {
                        kind: crate::jobs::assembly::DetectedArchiveKind::SevenZipSplit,
                        set_name: set_key.clone(),
                        volume_index: Some(normalized_index as u32),
                    });
            }
        }

        Ok(())
    }

    async fn scan_extraction_root(
        root: &Path,
        password: Option<String>,
    ) -> Result<Vec<ScannedExtractionFile>, String> {
        fn walk(
            root: &Path,
            current: &Path,
            files: &mut Vec<ScannedExtractionFile>,
        ) -> Result<(), String> {
            let entries = std::fs::read_dir(current)
                .map_err(|error| format!("failed to read {}: {error}", current.display()))?;
            for entry in entries {
                let entry = entry.map_err(|error| {
                    format!("failed to read entry in {}: {error}", current.display())
                })?;
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .map_err(|error| format!("failed to stat {}: {error}", path.display()))?;
                if file_type.is_dir() {
                    walk(root, &path, files)?;
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }

                let relative_path = path
                    .strip_prefix(root)
                    .map_err(|error| format!("failed to relativize {}: {error}", path.display()))?
                    .to_string_lossy()
                    .replace('\\', "/");
                let declared_role = weaver_model::files::FileRole::from_filename(&relative_path);
                let size = entry
                    .metadata()
                    .map_err(|error| format!("failed to stat {}: {error}", path.display()))?
                    .len();

                files.push(ScannedExtractionFile {
                    relative_path,
                    declared_role,
                    detected_archive: None,
                    size,
                });
            }
            Ok(())
        }

        let mut files = Vec::new();
        if root.exists() {
            walk(root, root, &mut files)?;
        }
        files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        Self::detect_nested_archive_identities(root, &mut files, password).await?;
        Ok(files)
    }

    fn clear_empty_dirs(root: &Path) -> Result<(), String> {
        fn prune(root: &Path, current: &Path) -> Result<bool, String> {
            let mut has_entries = false;
            let entries = std::fs::read_dir(current)
                .map_err(|error| format!("failed to read {}: {error}", current.display()))?;
            for entry in entries {
                let entry = entry.map_err(|error| {
                    format!("failed to read entry in {}: {error}", current.display())
                })?;
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .map_err(|error| format!("failed to stat {}: {error}", path.display()))?;
                if file_type.is_dir() {
                    if prune(root, &path)? {
                        has_entries = true;
                    }
                } else {
                    has_entries = true;
                }
            }

            if current != root && !has_entries {
                std::fs::remove_dir(current).map_err(|error| {
                    format!("failed to remove empty dir {}: {error}", current.display())
                })?;
                return Ok(false);
            }

            Ok(has_entries)
        }

        if root.exists() {
            let _ = prune(root, root)?;
        }
        Ok(())
    }

    pub(super) fn clear_persisted_extracted_members(&self, job_id: JobId) {
        if let Err(error) = self.db.clear_extracted_members(job_id) {
            warn!(
                job_id = job_id.0,
                error = %error,
                "failed to clear persisted extracted members before nested extraction"
            );
        }
    }

    fn rebuild_assembly_from_staging(
        &mut self,
        job_id: JobId,
        nested_archives: &[NestedArchiveFile],
    ) -> Result<Vec<(String, crate::jobs::assembly::ArchiveType)>, String> {
        let mut assembly = crate::jobs::assembly::JobAssembly::new(job_id);
        let mut detected_archives = HashMap::new();
        let mut file_identities = HashMap::new();
        let mut topologies = BTreeMap::<String, crate::jobs::assembly::ArchiveTopology>::new();

        for (index, archive) in nested_archives.iter().enumerate() {
            let file_index = u32::try_from(index)
                .map_err(|_| format!("too many nested archive files for job {}", job_id.0))?;
            let file_id = crate::jobs::ids::NzbFileId { job_id, file_index };
            let segment_sizes = Self::synthetic_segment_sizes(archive.size);
            let mut file_assembly = crate::jobs::assembly::FileAssembly::new(
                file_id,
                archive.relative_path.clone(),
                archive.declared_role.clone(),
                segment_sizes.clone(),
            );
            if let Some(detected_archive) = archive.detected_archive.clone() {
                detected_archives.insert(file_id.file_index, detected_archive);
            }
            let classification = archive.detected_archive.clone().or_else(|| {
                Self::nested_declared_archive_identity(
                    &archive.relative_path,
                    &archive.declared_role,
                )
            });
            file_identities.insert(
                file_id.file_index,
                crate::jobs::record::ActiveFileIdentity {
                    file_index,
                    source_filename: archive.relative_path.clone(),
                    current_filename: archive.relative_path.clone(),
                    canonical_filename: None,
                    classification,
                    classification_source: if archive.detected_archive.is_some() {
                        crate::jobs::FileIdentitySource::Nested
                    } else {
                        crate::jobs::FileIdentitySource::Declared
                    },
                },
            );
            for (segment_number, segment_size) in segment_sizes.into_iter().enumerate() {
                file_assembly
                    .commit_segment(segment_number as u32, segment_size)
                    .map_err(|error| {
                        format!(
                            "failed to mark nested archive {} complete: {error}",
                            archive.relative_path
                        )
                    })?;
            }
            assembly.add_file(file_assembly);

            let topology = topologies
                .entry(archive.set_name.clone())
                .or_insert_with(|| crate::jobs::assembly::ArchiveTopology {
                    archive_type: archive.archive_type,
                    volume_map: HashMap::new(),
                    complete_volumes: HashSet::new(),
                    expected_volume_count: Some(0),
                    members: Vec::new(),
                    unresolved_spans: Vec::new(),
                });
            if topology.archive_type != archive.archive_type {
                return Err(format!(
                    "conflicting nested archive types for set '{}'",
                    archive.set_name
                ));
            }
            topology
                .volume_map
                .insert(archive.relative_path.clone(), archive.volume_number);
            topology.complete_volumes.insert(archive.volume_number);
            topology.expected_volume_count = Some(
                topology
                    .expected_volume_count
                    .unwrap_or(0)
                    .max(archive.volume_number.saturating_add(1)),
            );
        }

        let mut to_extract = Vec::new();
        for (set_name, mut topology) in topologies {
            if topology.expected_volume_count == Some(0) {
                topology.expected_volume_count = Some(1);
            }
            to_extract.push((set_name.clone(), topology.archive_type));
            assembly.set_archive_topology(set_name, topology);
        }

        let previous_detected_keys: Vec<u32> = self
            .jobs
            .get(&job_id)
            .map(|state| state.detected_archives.keys().copied().collect())
            .unwrap_or_default();
        let previous_identity_keys: Vec<u32> = self
            .jobs
            .get(&job_id)
            .map(|state| state.file_identities.keys().copied().collect())
            .unwrap_or_default();

        {
            let state = self
                .jobs
                .get_mut(&job_id)
                .ok_or_else(|| format!("job {} not found", job_id.0))?;
            state.assembly = assembly;
            state.detected_archives = detected_archives.clone();
            state.file_identities = file_identities.clone();
        }

        for file_index in previous_detected_keys {
            if detected_archives.contains_key(&file_index) {
                continue;
            }
            self.db
                .delete_detected_archive_identity(job_id, file_index)
                .map_err(|error| {
                    format!(
                        "failed to delete nested detected archive identity for file {file_index}: {error}"
                    )
                })?;
        }
        for (file_index, identity) in &detected_archives {
            self.db
                .save_detected_archive_identity(job_id, *file_index, identity)
                .map_err(|error| {
                    format!(
                        "failed to save nested detected archive identity for file {file_index}: {error}"
                    )
                })?;
        }

        for file_index in previous_identity_keys {
            if file_identities.contains_key(&file_index) {
                continue;
            }
            self.db
                .delete_file_identity(job_id, file_index)
                .map_err(|error| {
                    format!("failed to delete nested file identity for file {file_index}: {error}")
                })?;
        }
        for identity in file_identities.values() {
            self.db
                .save_file_identity(job_id, identity)
                .map_err(|error| {
                    format!(
                        "failed to save nested file identity for file {}: {error}",
                        identity.file_index
                    )
                })?;
        }

        Ok(to_extract)
    }

    pub(crate) async fn maybe_start_nested_extraction(
        &mut self,
        job_id: JobId,
    ) -> Result<NestedExtractionDecision, String> {
        let current_archive_sources: HashSet<String> = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| format!("job {} not found", job_id.0))?
            .assembly
            .archive_topologies()
            .values()
            .flat_map(|topology| topology.volume_map.keys().cloned())
            .collect();
        let scan_root = self.nested_scan_root(job_id)?;
        let password = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.spec.password.clone());
        let scanned_files = Self::scan_extraction_root(&scan_root, password).await?;
        let nested_archives: Vec<NestedArchiveFile> = scanned_files
            .iter()
            .filter_map(|file| {
                if current_archive_sources.contains(&file.relative_path) {
                    return None;
                }
                let effective_role = file.effective_role();
                let archive_type = Self::archive_type_for_role(&effective_role)?;
                let set_name = file.archive_set_name()?;
                Some(NestedArchiveFile {
                    relative_path: file.relative_path.clone(),
                    declared_role: file.declared_role.clone(),
                    detected_archive: file.detected_archive.clone(),
                    archive_type,
                    set_name,
                    volume_number: Self::archive_volume_number(&effective_role),
                    size: file.size,
                })
            })
            .collect();

        if nested_archives.is_empty() {
            return Ok(NestedExtractionDecision::NoNestedArchives);
        }

        let current_depth = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| format!("job {} not found", job_id.0))?
            .extraction_depth;
        if current_depth.saturating_add(1) >= MAX_NESTED_EXTRACTION_DEPTH {
            warn!(
                job_id = job_id.0,
                depth = current_depth,
                archives = nested_archives.len(),
                "nested archive extraction depth limit reached; leaving archive output in place"
            );
            return Ok(NestedExtractionDecision::PreserveOutputsAtDepthLimit);
        }

        for file in &scanned_files {
            if Self::archive_type_for_role(&file.effective_role()).is_some() {
                continue;
            }
            let path = scan_root.join(&file.relative_path);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "failed to remove intermediate extracted file {}: {error}",
                        path.display()
                    ));
                }
            }
        }
        Self::clear_empty_dirs(&scan_root)?;

        self.clear_job_extraction_runtime(job_id);
        self.clear_job_rar_runtime(job_id);
        self.replace_failed_extraction_members(job_id, HashSet::new());
        self.clear_persisted_extracted_members(job_id);

        let to_extract = self.rebuild_assembly_from_staging(job_id, &nested_archives)?;
        if to_extract.is_empty() {
            return Ok(NestedExtractionDecision::NoNestedArchives);
        }

        let nested_file_ids: Vec<crate::jobs::ids::NzbFileId> = self
            .jobs
            .get(&job_id)
            .map(|state| state.assembly.files().map(|file| file.file_id()).collect())
            .unwrap_or_default();
        for file_id in nested_file_ids {
            self.refresh_archive_state_for_completed_file(job_id, file_id, false)
                .await;
        }

        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.extraction_depth = state.extraction_depth.saturating_add(1);
        }

        self.transition_post_state(job_id, crate::jobs::model::PostState::Extracting);

        info!(
            job_id = job_id.0,
            depth = current_depth + 1,
            archives = to_extract.len(),
            root = %scan_root.display(),
            "starting nested archive extraction"
        );

        let spawned = self.spawn_extractions(job_id, &to_extract).await;
        if spawned == 0 {
            return Err("nested archive extraction could not be started".to_string());
        }

        Ok(NestedExtractionDecision::Started)
    }
}
