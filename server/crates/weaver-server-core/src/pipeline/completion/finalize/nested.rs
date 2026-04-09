use super::*;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

impl Pipeline {
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

    fn scan_extraction_root(root: &Path) -> Result<Vec<ScannedExtractionFile>, String> {
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
                let role = weaver_model::files::FileRole::from_filename(&relative_path);
                let size = entry
                    .metadata()
                    .map_err(|error| format!("failed to stat {}: {error}", path.display()))?
                    .len();

                files.push(ScannedExtractionFile {
                    relative_path,
                    role,
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
        let mut topologies = BTreeMap::<String, crate::jobs::assembly::ArchiveTopology>::new();

        for (index, archive) in nested_archives.iter().enumerate() {
            let file_index = u32::try_from(index)
                .map_err(|_| format!("too many nested archive files for job {}", job_id.0))?;
            let file_id = crate::jobs::ids::NzbFileId { job_id, file_index };
            let segment_sizes = Self::synthetic_segment_sizes(archive.size);
            let mut file_assembly = crate::jobs::assembly::FileAssembly::new(
                file_id,
                archive.relative_path.clone(),
                archive.role.clone(),
                segment_sizes.clone(),
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

        let state = self
            .jobs
            .get_mut(&job_id)
            .ok_or_else(|| format!("job {} not found", job_id.0))?;
        state.assembly = assembly;
        Ok(to_extract)
    }

    pub(super) async fn maybe_start_nested_extraction(
        &mut self,
        job_id: JobId,
    ) -> Result<bool, String> {
        let scan_root = self.nested_scan_root(job_id)?;
        let scanned_files = Self::scan_extraction_root(&scan_root)?;
        let nested_archives: Vec<NestedArchiveFile> = scanned_files
            .iter()
            .filter_map(|file| {
                let archive_type = Self::archive_type_for_role(&file.role)?;
                let set_name =
                    weaver_model::files::archive_base_name(&file.relative_path, &file.role)?;
                Some(NestedArchiveFile {
                    relative_path: file.relative_path.clone(),
                    role: file.role.clone(),
                    archive_type,
                    set_name,
                    volume_number: Self::archive_volume_number(&file.role),
                    size: file.size,
                })
            })
            .collect();

        if nested_archives.is_empty() {
            return Ok(false);
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
            return Ok(false);
        }

        for file in &scanned_files {
            if Self::archive_type_for_role(&file.role).is_some() {
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
            return Ok(false);
        }

        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.extraction_depth = state.extraction_depth.saturating_add(1);
        }

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

        Ok(true)
    }
}
