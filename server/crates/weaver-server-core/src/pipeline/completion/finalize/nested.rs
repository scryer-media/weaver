use super::*;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

mod worker;

fn scan_extraction_directory(
    root: &Path,
    current: &Path,
    files: &mut Vec<ScannedExtractionFile>,
) -> Result<(), String> {
    let entries = match std::fs::read_dir(current) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("failed to read {}: {error}", current.display())),
    };
    scan_extraction_children(root, current, files, entries)
}

fn scan_extraction_children(
    root: &Path,
    current: &Path,
    files: &mut Vec<ScannedExtractionFile>,
    entries: impl IntoIterator<Item = std::io::Result<std::fs::DirEntry>>,
) -> Result<(), String> {
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("failed to read entry in {}: {error}", current.display()))?;
        let path = entry.path();
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            // Direct-store cleanup can remove a partial after enumeration.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("failed to stat {}: {error}", path.display())),
        };
        if metadata.is_dir() {
            scan_extraction_directory(root, &path, files)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(format!(
                "extraction tree contains non-file/non-directory entry {}",
                path.display()
            ));
        }
        let relative_path = path
            .strip_prefix(root)
            .map_err(|error| format!("failed to relativize {}: {error}", path.display()))?
            .to_string_lossy()
            .replace('\\', "/");
        let declared_role = weaver_model::files::FileRole::from_filename(&relative_path);
        files.push(ScannedExtractionFile {
            relative_path,
            declared_role,
            detected_archive: None,
        });
    }
    Ok(())
}

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
            weaver_model::files::FileRole::TarXzArchive => {
                Some(crate::jobs::assembly::ArchiveType::TarXz)
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
            weaver_model::files::FileRole::XzArchive => {
                Some(crate::jobs::assembly::ArchiveType::Xz)
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

    async fn detect_nested_archive_identities(
        root: &Path,
        files: &mut [ScannedExtractionFile],
        password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
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
            if let Ok(facts) = Pipeline::parse_rar_volume_facts_from_path(
                path.clone(),
                password_candidates.clone(),
            )
            .await
            {
                file.detected_archive = Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: set_key,
                    volume_index: facts.volume_number,
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
        password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
        working_root: bool,
    ) -> Result<Vec<ScannedExtractionFile>, String> {
        let mut files = Vec::new();
        if root.exists() {
            let entries = std::fs::read_dir(root)
                .map_err(|error| format!("failed to read extraction root: {error}"))?;
            let entries = entries.filter(|entry| {
                !entry.as_ref().is_ok_and(|entry| {
                    entry.file_name() == ".weaver-nested"
                        || (working_root
                            && entry.file_name()
                                == crate::pipeline::direct_store::manifest::DIRECTORY)
                })
            });
            scan_extraction_children(root, root, &mut files, entries)?;
        }
        files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        Self::detect_nested_archive_identities(root, &mut files, password_candidates).await?;
        Ok(files)
    }

    pub(super) async fn clear_persisted_extracted_members(&self, job_id: JobId) {
        if let Err(error) = self
            .db_blocking(move |db| db.clear_extracted_members(job_id))
            .await
        {
            warn!(
                job_id = job_id.0,
                error = %error,
                "failed to clear persisted extracted members before nested extraction"
            );
        }
    }

    pub(crate) async fn maybe_start_nested_extraction(
        &mut self,
        job_id: JobId,
    ) -> Result<NestedExtractionDecision, String> {
        if self.shared_state.is_post_processing_paused() {
            self.deferred_post_processing.insert(job_id);
            return Ok(NestedExtractionDecision::Deferred);
        }
        self.retire_unconsumed_chase_streams(job_id).await;
        self.resume_scoped_replay(job_id).await?;
        if self
            .archive_runtime
            .get(&job_id)
            .is_some_and(|runtime| runtime.pending())
        {
            return Ok(NestedExtractionDecision::Started);
        }
        let mut current_archive_sources: HashSet<String> = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| format!("job {} not found", job_id.0))?
            .assembly
            .archive_topologies()
            .values()
            .flat_map(|topology| topology.volume_map.keys().cloned())
            .collect();
        // Verified recovery can retire an opaque damaged source before it
        // ever acquires an archive topology. It is still an outer input, not
        // an archive member produced by extraction.
        if let Some(state) = self.jobs.get(&job_id) {
            for file in state.assembly.files() {
                if self.recovery_superseded_source(job_id, file.file_id()) {
                    current_archive_sources
                        .insert(self.current_filename_for_file(job_id, file).to_string());
                }
            }
        }
        if let Some(runtime) = self.archive_runtime.get(&job_id) {
            current_archive_sources.extend(runtime.consumed.iter().cloned());
        }
        let scan_root = self.nested_scan_root(job_id)?;
        if self
            .jobs
            .get(&job_id)
            .and_then(|state| state.staging_dir.as_ref())
            .is_some_and(|staging| staging == &scan_root)
        {
            // Opening the root and walking the whole tree is filesystem work
            // proportional to the job's output; it must not run on the
            // pipeline task. The errors, rejections included, are the walk's
            // own, unchanged.
            let budget = self.extraction_budget(job_id, &scan_root)?;
            let root_for_scan = scan_root.clone();
            tokio::task::spawn_blocking(move || {
                ExtractionRoot::open(&root_for_scan)?
                    .scan_no_links(&budget)
                    .map_err(String::from)
            })
            .await
            .map_err(|error| format!("extraction root link scan task failed: {error}"))??;
        }
        let password_candidates = self.archive_password_candidates_for_job(job_id).candidates;
        let working_root = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.working_dir == scan_root);
        let scanned_files =
            Self::scan_extraction_root(&scan_root, password_candidates, working_root).await?;
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
                    archive_type,
                    set_name,
                    volume_number: Self::archive_volume_number(&effective_role),
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

        let mut groups = BTreeMap::<String, Vec<NestedArchiveFile>>::new();
        for archive in nested_archives {
            groups
                .entry(archive.set_name.clone())
                .or_default()
                .push(archive);
        }
        for (set_name, mut files) in groups {
            files.sort_by_key(|file| file.volume_number);
            self.spawn_scoped_filesystem_extraction(job_id, &scan_root, set_name, files)
                .await?;
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.extraction_depth = state.extraction_depth.saturating_add(1);
        }
        self.transition_postprocessing_status(job_id, JobStatus::Extracting, Some("extracting"));

        Ok(NestedExtractionDecision::Started)
    }
}

#[cfg(test)]
mod cleanup_scan_tests {
    use super::*;

    #[test]
    fn nested_scan_ignores_entries_removed_after_enumeration() {
        for nested in [false, true] {
            for remove_directory in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let current = if nested {
                    root.path().join("nested")
                } else {
                    root.path().to_path_buf()
                };
                std::fs::create_dir_all(&current).unwrap();
                std::fs::write(current.join("kept.bin"), b"keep").unwrap();
                let removed = current.join("partial");
                if remove_directory {
                    std::fs::create_dir(&removed).unwrap();
                } else {
                    std::fs::write(&removed, b"discard").unwrap();
                }
                let entries: Vec<_> = std::fs::read_dir(&current).unwrap().collect();
                assert_eq!(entries.len(), 2);
                if remove_directory {
                    std::fs::remove_dir(&removed).unwrap();
                } else {
                    std::fs::remove_file(&removed).unwrap();
                }
                let mut files = Vec::new();
                scan_extraction_children(root.path(), &current, &mut files, entries).unwrap();
                assert_eq!(files.len(), 1);
                assert_eq!(
                    files[0].relative_path,
                    if nested {
                        "nested/kept.bin"
                    } else {
                        "kept.bin"
                    }
                );
            }
        }
    }

    #[test]
    fn nested_scan_tolerates_directory_removal_after_metadata() {
        let root = tempfile::tempdir().unwrap();
        let child = root.path().join("partial");
        std::fs::create_dir(&child).unwrap();
        assert!(std::fs::symlink_metadata(&child).unwrap().is_dir());
        std::fs::remove_dir(&child).unwrap();
        let mut files = Vec::new();
        scan_extraction_directory(root.path(), &child, &mut files).unwrap();
        assert!(files.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn nested_scan_rejects_dangling_links_after_cleanup() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a.partial"), b"discard").unwrap();
        symlink("a.partial", root.path().join("z.link")).unwrap();
        let mut entries: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(Result::unwrap)
            .collect();
        entries.sort_by_key(|entry| entry.file_name());
        std::fs::remove_file(root.path().join("a.partial")).unwrap();
        let mut files = Vec::new();
        let error = scan_extraction_children(
            root.path(),
            root.path(),
            &mut files,
            entries.into_iter().map(Ok),
        )
        .unwrap_err();
        assert!(error.contains("non-file/non-directory"), "{error}");
        assert!(files.is_empty());
    }

    #[test]
    fn nested_scan_preserves_entry_read_errors() {
        let root = tempfile::tempdir().unwrap();
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::Other,
        ] {
            let mut files = Vec::new();
            let error = scan_extraction_children(
                root.path(),
                root.path(),
                &mut files,
                [Err(std::io::Error::new(kind, "entry enumeration failed"))],
            )
            .unwrap_err();
            assert!(error.contains("entry enumeration failed"), "{error}");
            assert!(files.is_empty());
        }
    }
}
