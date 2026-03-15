use super::*;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Simple archive type for non-RAR, non-7z extraction.
#[derive(Debug, Clone, Copy)]
pub(super) enum SimpleArchiveKind {
    Zip,
    Tar,
    TarGz,
    Gz,
    Split,
}

fn extract_zip(
    archive_path: &Path,
    output_dir: &Path,
    event_tx: &tokio::sync::broadcast::Sender<PipelineEvent>,
    job_id: JobId,
    set_name: &str,
) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(archive_path).map_err(|e| format!("failed to open zip: {e}"))?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| format!("failed to read zip archive: {e}"))?;
    let mut extracted = Vec::new();

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| format!("failed to read zip entry {i}: {e}"))?;
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

    let mut reader = weaver_core::split_reader::SplitFileReader::open(file_paths)
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

fn move_path_with_copy_fallback(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(src)?;

    if metadata.is_dir() {
        if dst.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dst.display()),
            ));
        }

        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            move_path_with_copy_fallback(&entry.path(), &dst.join(entry.file_name()))?;
        }
        std::fs::remove_dir(src)?;
        return Ok(());
    }

    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(src, dst)?;
    std::fs::remove_file(src)?;
    Ok(())
}

impl Pipeline {
    async fn cleanup_par2_files(&self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let cleanup_dir = state.working_dir.clone();
        let par2_files: Vec<String> = state
            .assembly
            .files()
            .filter(|f| matches!(f.role(), weaver_core::classify::FileRole::Par2 { .. }))
            .map(|f| f.filename().to_string())
            .collect();
        if par2_files.is_empty() {
            return;
        }

        let mut removed = 0u32;
        for filename in &par2_files {
            let path = cleanup_dir.join(filename);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(file = %path.display(), error = %e, "failed to delete PAR2 file");
                }
            }
        }
        if removed > 0 {
            info!(
                job_id = job_id.0,
                removed,
                total = par2_files.len(),
                "deleted PAR2 files"
            );
        }
    }

    pub(super) fn clear_par2_runtime_state(&mut self, job_id: JobId) {
        self.par2_runtime.remove(&job_id);
    }

    async fn compute_complete_destination(
        &self,
        job_id: JobId,
        job_name: &str,
        category: Option<&str>,
    ) -> PathBuf {
        let dir_name = job::sanitize_dirname(job_name);
        let base_dest = {
            let cfg = self.config.read().await;
            let cat_dest = category.and_then(|cat| {
                cfg.categories
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(cat))
                    .and_then(|c| c.dest_dir.as_ref())
                    .filter(|d| !d.is_empty())
                    .map(PathBuf::from)
            });

            if let Some(custom_dest) = cat_dest {
                custom_dest.join(&dir_name)
            } else {
                let mut dest = self.complete_dir.clone();
                if let Some(cat) = category
                    && !cat.is_empty()
                {
                    dest = dest.join(cat);
                }
                dest.join(&dir_name)
            }
        };

        if !base_dest.exists() {
            return base_dest;
        }

        let parent = base_dest
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let suffixed = parent.join(format!("{}.#{}", dir_name, job_id.0));
        if !suffixed.exists() {
            return suffixed;
        }

        let mut attempt = 1u32;
        loop {
            let candidate = parent.join(format!("{}.#{}.{}", dir_name, job_id.0, attempt));
            if !candidate.exists() {
                return candidate;
            }
            attempt += 1;
        }
    }

    /// Move extracted/completed files from the intermediate working directory
    /// to the complete directory, organized by category.
    ///
    /// Layout: `{complete_dir}/[{category}/]{job_name}/`
    /// On collision, appends `.#<job_id>` (and a numeric suffix if needed).
    ///
    /// Uses rename() for same-filesystem moves, falls back to copy+delete for cross-FS.
    pub(super) async fn move_to_complete(&mut self, job_id: JobId) -> Result<PathBuf, String> {
        let (working_dir, job_name, category) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Err(format!("job {} not found for move_to_complete", job_id.0));
            };
            (
                state.working_dir.clone(),
                state.spec.name.clone(),
                state.spec.category.clone(),
            )
        };

        let _ = self
            .event_tx
            .send(PipelineEvent::MoveToCompleteStarted { job_id });

        let dest = self
            .compute_complete_destination(job_id, &job_name, category.as_deref())
            .await;

        // List files in the working directory before creating the destination so
        // a missing source directory doesn't leave behind an empty complete dir.
        let mut entries = match tokio::fs::read_dir(&working_dir).await {
            Ok(entries) => entries,
            Err(e) => {
                return Err(format!(
                    "failed to read working directory {} for move: {e}",
                    working_dir.display()
                ));
            }
        };

        // Ensure destination exists.
        if let Err(e) = tokio::fs::create_dir_all(&dest).await {
            return Err(format!(
                "failed to create complete directory {}: {e}",
                dest.display()
            ));
        }

        let mut moved = 0u32;
        let mut failures = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let src = entry.path();
            let file_name = entry.file_name();
            if file_name == ".weaver-chunks" {
                if let Err(error) = tokio::fs::remove_dir_all(&src).await
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(
                        path = %src.display(),
                        error = %error,
                        "failed to remove extraction chunk workspace before final move"
                    );
                }
                continue;
            }
            let dst = dest.join(&file_name);

            // Try rename first (fast, same filesystem).
            match tokio::fs::rename(&src, &dst).await {
                Ok(()) => {
                    moved += 1;
                }
                Err(rename_err) => {
                    let src_fallback = src.clone();
                    let dst_fallback = dst.clone();
                    match tokio::task::spawn_blocking(move || {
                        move_path_with_copy_fallback(&src_fallback, &dst_fallback)
                    })
                    .await
                    {
                        Ok(Ok(())) => {
                            moved += 1;
                        }
                        Ok(Err(copy_err)) => {
                            failures.push(format!(
                                "{}: rename failed: {}; fallback failed: {}",
                                file_name.to_string_lossy(),
                                rename_err,
                                copy_err
                            ));
                        }
                        Err(join_err) => {
                            failures.push(format!(
                                "{}: rename failed: {}; fallback task failed: {}",
                                file_name.to_string_lossy(),
                                rename_err,
                                join_err
                            ));
                        }
                    }
                }
            }
        }

        if !failures.is_empty() {
            for failure in &failures {
                warn!(job_id = job_id.0, error = %failure, "failed to move entry to complete directory");
            }
            return Err(format!(
                "failed to move {} entr{} to complete directory",
                failures.len(),
                if failures.len() == 1 { "y" } else { "ies" }
            ));
        }

        // Remove the now-empty intermediate directory.
        if let Err(e) = tokio::fs::remove_dir(&working_dir).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                job_id = job_id.0,
                dir = %working_dir.display(),
                error = %e,
                "failed to remove intermediate directory after move"
            );
        }

        // Update working_dir to point to the complete path (for API reporting).
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.working_dir = dest.clone();
        }

        info!(
            job_id = job_id.0,
            moved,
            dest = %dest.display(),
            "moved files to complete directory"
        );
        let _ = self
            .event_tx
            .send(PipelineEvent::MoveToCompleteFinished { job_id });
        Ok(dest)
    }

    fn rar_volume_numbers_by_filename(&self, job_id: JobId) -> HashMap<String, u32> {
        let mut volume_numbers = HashMap::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return volume_numbers;
        };

        for topology in state.assembly.archive_topologies().values() {
            for (filename, &volume_number) in &topology.volume_map {
                volume_numbers.insert(filename.clone(), volume_number);
            }
        }

        volume_numbers
    }

    pub(super) fn claim_clean_rar_volume(
        decision: &crate::pipeline::rar_state::RarVolumeDeleteDecision,
    ) -> bool {
        decision.pending_owners.is_empty()
            && decision.failed_owners.is_empty()
            && !decision.unresolved_boundary
    }

    pub(super) fn suspect_rar_volumes_for_job(&self, job_id: JobId) -> HashSet<u32> {
        let suspect: HashSet<u32> = self
            .rar_sets
            .iter()
            .filter(|((jid, _), _)| *jid == job_id)
            .flat_map(|(_, state)| {
                let mut volumes = state
                    .verified_suspect_volumes
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                if let Some(plan) = state.plan.as_ref() {
                    volumes.extend(plan.delete_decisions.iter().filter_map(
                        |(volume, decision)| {
                            (!decision.failed_owners.is_empty()
                                || !decision.pending_owners.is_empty()
                                || plan.waiting_on_volumes.contains(volume))
                            .then_some(*volume)
                        },
                    ));
                }
                volumes
            })
            .collect();
        suspect
    }

    pub(super) fn apply_eager_delete_exclusions(
        &self,
        job_id: JobId,
        verification: &mut weaver_par2::VerificationResult,
    ) -> (u32, u32) {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();
        let suspect_volumes = self.suspect_rar_volumes_for_job(job_id);
        let volume_numbers = self.rar_volume_numbers_by_filename(job_id);

        let mut skipped_blocks = 0u32;
        let mut retained_suspect_blocks = 0u32;
        for file_verification in &mut verification.files {
            if matches!(
                file_verification.status,
                weaver_par2::verify::FileStatus::Missing
            ) && eagerly_deleted_names.contains(file_verification.filename.as_str())
            {
                let Some(&volume_number) = volume_numbers.get(file_verification.filename.as_str())
                else {
                    continue;
                };
                if suspect_volumes.contains(&volume_number) {
                    retained_suspect_blocks = retained_suspect_blocks
                        .saturating_add(file_verification.missing_slice_count);
                    continue;
                }
                skipped_blocks += file_verification.missing_slice_count;
                file_verification.status = weaver_par2::verify::FileStatus::Complete;
                file_verification.valid_slices.fill(true);
                file_verification.missing_slice_count = 0;
            }
        }
        verification.total_missing_blocks = verification
            .total_missing_blocks
            .saturating_sub(skipped_blocks);
        verification.refresh_repairability();
        (skipped_blocks, retained_suspect_blocks)
    }

    pub(super) fn recompute_volume_safety_from_verification(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();
        let suspect_volumes = self.suspect_rar_volumes_for_job(job_id);
        let volume_numbers = self.rar_volume_numbers_by_filename(job_id);

        let status_by_name: HashMap<&str, &weaver_par2::FileVerification> = verification
            .files
            .iter()
            .map(|file| (file.filename.as_str(), file))
            .collect();

        let plans: Vec<(String, HashSet<u32>)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .archive_topologies()
                .iter()
                .map(|(set_name, topo)| {
                    let mut suspect = HashSet::new();
                    for (filename, &volume_number) in &topo.volume_map {
                        if let Some(file) = status_by_name.get(filename.as_str()) {
                            match file.status {
                                weaver_par2::verify::FileStatus::Complete
                                | weaver_par2::verify::FileStatus::Renamed(_) => {}
                                weaver_par2::verify::FileStatus::Missing
                                    if eagerly_deleted_names.contains(filename.as_str())
                                        && !volume_numbers.get(filename.as_str()).is_some_and(
                                            |number| suspect_volumes.contains(number),
                                        ) => {}
                                weaver_par2::verify::FileStatus::Missing
                                | weaver_par2::verify::FileStatus::Damaged(_) => {
                                    suspect.insert(volume_number);
                                }
                            }
                        }
                    }
                    (set_name.clone(), suspect)
                })
                .collect()
        };

        if let Err(error) = self.db.clear_verified_suspect_volumes(job_id) {
            error!(
                job_id = job_id.0,
                error = %error,
                "failed to clear persisted verified suspect RAR volumes"
            );
        }

        for (set_name, suspect) in plans {
            if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                state.verified_suspect_volumes = suspect.clone();
            }
            self.persist_verified_suspect_volumes(job_id, &set_name, &suspect);
        }
    }

    pub(super) async fn refresh_rar_topology_after_normalization(
        &mut self,
        job_id: JobId,
        normalized_files: &HashSet<String>,
    ) -> Result<(), String> {
        if normalized_files.is_empty() {
            return Ok(());
        }

        let touched_sets: BTreeMap<String, HashSet<String>> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };

            state
                .assembly
                .files()
                .filter_map(|file| {
                    if !normalized_files.contains(file.filename()) {
                        return None;
                    }
                    match file.role() {
                        weaver_core::classify::FileRole::RarVolume { .. } => {
                            weaver_core::classify::archive_base_name(file.filename(), file.role())
                                .map(|set_name| (set_name, file.filename().to_string()))
                        }
                        _ => None,
                    }
                })
                .fold(BTreeMap::new(), |mut acc, (set_name, filename)| {
                    acc.entry(set_name).or_default().insert(filename);
                    acc
                })
        };

        let mut errors = Vec::new();
        for (set_name, touched_filenames) in touched_sets {
            match self
                .refresh_rar_volume_facts_for_set(job_id, &set_name, &touched_filenames)
                .await
            {
                Ok(()) => info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "refreshed RAR topology after normalization"
                ),
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed to refresh RAR topology after normalization; retaining previous snapshot and topology"
                    );
                    errors.push(format!("{set_name}: {error}"));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    fn has_active_rar_workers(&self, job_id: JobId) -> bool {
        self.rar_set_names_for_job(job_id).iter().any(|set_name| {
            self.rar_sets
                .get(&(job_id, set_name.clone()))
                .is_some_and(|state| state.active_workers > 0)
        })
    }

    fn placement_normalized_files(plan: &weaver_par2::PlacementPlan) -> HashSet<String> {
        let mut normalized_files = HashSet::new();
        for (left, right) in &plan.swaps {
            normalized_files.insert(left.correct_name.clone());
            normalized_files.insert(right.correct_name.clone());
        }
        for entry in &plan.renames {
            normalized_files.insert(entry.correct_name.clone());
        }
        normalized_files
    }

    fn log_placement_plan(job_id: JobId, plan: &weaver_par2::PlacementPlan) {
        if plan.swaps.is_empty() && plan.renames.is_empty() {
            return;
        }

        let swap_pairs: Vec<String> = plan
            .swaps
            .iter()
            .map(|(left, right)| {
                format!(
                    "{} -> {} | {} -> {}",
                    left.current_name, left.correct_name, right.current_name, right.correct_name
                )
            })
            .collect();
        let renames: Vec<String> = plan
            .renames
            .iter()
            .map(|entry| format!("{} -> {}", entry.current_name, entry.correct_name))
            .collect();

        info!(
            job_id = job_id.0,
            swaps = ?swap_pairs,
            renames = ?renames,
            "placement scan identified remapped files"
        );
    }

    async fn apply_placement_plan_for_retry_or_repair(
        &mut self,
        job_id: JobId,
        working_dir: PathBuf,
        plan: &weaver_par2::PlacementPlan,
    ) -> Result<(), String> {
        if plan.swaps.is_empty() && plan.renames.is_empty() {
            return Ok(());
        }

        let plan = plan.clone();
        let normalized_files = Self::placement_normalized_files(&plan);
        let plan_for_apply = plan.clone();
        let moved = tokio::task::spawn_blocking(move || {
            weaver_par2::apply_placement_plan(&working_dir, &plan_for_apply)
                .map_err(|e| format!("placement normalization failed: {e}"))
        })
        .await
        .map_err(|e| format!("placement normalization task panicked: {e}"))??;

        info!(
            job_id = job_id.0,
            swaps = plan.swaps.len(),
            renames = plan.renames.len(),
            moved,
            "applied placement normalization after verify"
        );

        self.refresh_rar_topology_after_normalization(job_id, &normalized_files)
            .await
    }

    async fn recompute_rar_retry_frontier(&mut self, job_id: JobId) {
        for set_name in self.rar_set_names_for_job(job_id) {
            if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error = %error,
                    "failed to recompute RAR set while rebuilding retry frontier"
                );
            }
        }
    }

    pub(super) fn invalid_rar_retry_frontier_reason(&self, job_id: JobId) -> Option<String> {
        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut has_incomplete_sets = false;

        for set_name in self.rar_set_names_for_job(job_id) {
            let Some(set_state) = self.rar_sets.get(&(job_id, set_name.clone())) else {
                continue;
            };
            let Some(plan) = set_state.plan.as_ref() else {
                continue;
            };
            let set_complete = !plan.member_names.is_empty()
                && plan
                    .member_names
                    .iter()
                    .all(|member| extracted.contains(member));
            if set_complete {
                continue;
            }

            has_incomplete_sets = true;

            let waiting_marked_deletable: Vec<u32> = plan
                .waiting_on_volumes
                .intersection(&plan.deletion_eligible)
                .copied()
                .collect();
            if !waiting_marked_deletable.is_empty() {
                return Some(format!(
                    "set '{set_name}' waiting volumes marked deletable: {:?}",
                    waiting_marked_deletable
                ));
            }

            let waiting_already_deleted: Vec<u32> = plan
                .waiting_on_volumes
                .iter()
                .copied()
                .filter(|volume| {
                    self.is_rar_volume_deleted(job_id, &plan.topology.volume_map, *volume)
                })
                .collect();
            if !waiting_already_deleted.is_empty() {
                return Some(format!(
                    "set '{set_name}' waiting volumes already deleted: {:?}",
                    waiting_already_deleted
                ));
            }

            if !plan.ready_members.is_empty()
                || matches!(
                    plan.phase,
                    crate::pipeline::rar_state::RarSetPhase::FallbackFullSet
                )
            {
                return None;
            }
        }

        if has_incomplete_sets {
            Some("no retryable work remains for incomplete RAR sets".to_string())
        } else {
            None
        }
    }

    fn job_has_only_rar_archives(&self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };

        let mut has_rar = false;
        for file in state.assembly.files() {
            match file.role() {
                weaver_core::classify::FileRole::RarVolume { .. } => has_rar = true,
                weaver_core::classify::FileRole::SevenZipArchive
                | weaver_core::classify::FileRole::SevenZipSplit { .. } => return false,
                _ => {}
            }
        }

        has_rar
    }

    fn rar_set_names_for_job(&self, job_id: JobId) -> Vec<String> {
        let mut set_names: HashSet<String> = HashSet::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        for file in state.assembly.files() {
            if matches!(
                file.role(),
                weaver_core::classify::FileRole::RarVolume { .. }
            ) && let Some(set_name) =
                weaver_core::classify::archive_base_name(file.filename(), file.role())
            {
                set_names.insert(set_name);
            }
        }
        let mut set_names: Vec<String> = set_names.into_iter().collect();
        set_names.sort();
        set_names
    }

    async fn finalize_completed_archive_job(&mut self, job_id: JobId) {
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.status == JobStatus::Extracting)
        {
            self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
        }
        let _ = self
            .event_tx
            .send(PipelineEvent::ExtractionComplete { job_id });

        {
            let state = self.jobs.get(&job_id).unwrap();
            let cleanup_dir = state.working_dir.clone();
            let cleanup_files: Vec<String> = state
                .assembly
                .files()
                .filter(|f| {
                    matches!(
                        f.role(),
                        weaver_core::classify::FileRole::Par2 { .. }
                            | weaver_core::classify::FileRole::RarVolume { .. }
                            | weaver_core::classify::FileRole::SevenZipArchive
                            | weaver_core::classify::FileRole::SevenZipSplit { .. }
                    )
                })
                .map(|f| f.filename().to_string())
                .collect();
            let mut removed = 0u32;
            for filename in &cleanup_files {
                let path = cleanup_dir.join(filename);
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => removed += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        warn!(
                            file = %path.display(),
                            error = %e,
                            "failed to clean up source file"
                        );
                    }
                }
            }
            info!(
                job_id = job_id.0,
                removed,
                total = cleanup_files.len(),
                "post-extraction cleanup complete"
            );
        }

        let was_extracting = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.status == JobStatus::Extracting);
        if let Err(error) = self.move_to_complete(job_id).await {
            if was_extracting {
                self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
            }
            self.fail_job(job_id, error);
            return;
        }

        {
            let state = self.jobs.get_mut(&job_id).unwrap();
            state.status = JobStatus::Complete;
        }
        // Ensure DownloadFinished is journaled before JobCompleted so the
        // timeline shows an accurate download duration.
        if self.active_download_passes.remove(&job_id) {
            let _ = self
                .event_tx
                .send(PipelineEvent::DownloadFinished { job_id });
        }
        self.clear_par2_runtime_state(job_id);
        self.clear_job_rar_runtime(job_id);
        self.job_order.retain(|id| *id != job_id);
        let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
        self.record_job_history(job_id);
    }

    async fn check_rar_job_completion(&mut self, job_id: JobId) {
        let set_names = self.rar_set_names_for_job(job_id);
        if set_names.is_empty() {
            return;
        }

        let has_active_worker = set_names.iter().any(|set_name| {
            self.rar_sets
                .get(&(job_id, set_name.clone()))
                .is_some_and(|state| state.active_workers > 0)
        });
        if has_active_worker {
            if let Some(state) = self.jobs.get_mut(&job_id)
                && state.status != JobStatus::Extracting
            {
                state.status = JobStatus::Extracting;
                if let Err(error) = self.db.set_active_job_status(job_id, "extracting", None) {
                    error!(error = %error, "db write failed for extracting status");
                }
                self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionReady { job_id });
            }
            return;
        }

        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut fallback_sets = Vec::new();
        let mut has_incomplete_sets = false;
        let mut has_ready_incremental_work = false;
        for set_name in &set_names {
            let set_state = self.rar_sets.get(&(job_id, set_name.clone()));
            let set_complete =
                set_state
                    .and_then(|state| state.plan.as_ref())
                    .is_some_and(|plan| {
                        !plan.member_names.is_empty()
                            && plan
                                .member_names
                                .iter()
                                .all(|member| extracted.contains(member))
                    });
            if set_complete {
                self.extracted_sets
                    .entry(job_id)
                    .or_default()
                    .insert(set_name.clone());
            } else {
                has_incomplete_sets = true;
                if let Some(state) = set_state
                    && let Some(plan) = state.plan.as_ref()
                {
                    if matches!(
                        plan.phase,
                        crate::pipeline::rar_state::RarSetPhase::FallbackFullSet
                    ) {
                        fallback_sets.push(set_name.clone());
                    } else if !plan.ready_members.is_empty() {
                        has_ready_incremental_work = true;
                    }
                }
            }
        }

        if has_incomplete_sets {
            if let Some(state) = self.jobs.get_mut(&job_id)
                && state.status != JobStatus::Extracting
            {
                state.status = JobStatus::Extracting;
                if let Err(error) = self.db.set_active_job_status(job_id, "extracting", None) {
                    error!(error = %error, "db write failed for extracting status");
                }
                self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionReady { job_id });
            }

            if has_ready_incremental_work {
                self.try_partial_extraction(job_id).await;
                return;
            }

            for set_name in &fallback_sets {
                if let Err(error) = self.extract_rar_set(job_id, set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "failed to start RAR full-set extraction"
                    );
                    self.fail_job(job_id, error);
                    return;
                }
            }
            if !fallback_sets.is_empty() {
                return;
            }

            return;
        }

        self.finalize_completed_archive_job(job_id).await;
    }

    /// Check if all data files in a job are complete, and trigger post-processing.
    ///
    /// PAR2 is treated as a repair tool only — damage is detected via yEnc CRC
    /// (per-segment) and RAR CRC (per-member extraction). If
    /// CRC failures occur, recovery files are promoted for download and repair
    /// runs from disk using `verify_all` + `plan_repair` + `execute_repair`.
    pub(super) async fn check_job_completion(&mut self, job_id: JobId) {
        // Step 1: Are all data files (non-recovery) complete?
        {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let total = state.assembly.data_file_count();
            let complete = state.assembly.complete_data_file_count();
            if complete < total {
                return;
            }
            // If no data files registered yet but there are still segments queued,
            // downloads haven't really started — don't prematurely leave Downloading.
            if total == 0
                && state.status == JobStatus::Downloading
                && (!state.download_queue.is_empty() || !state.recovery_queue.is_empty())
            {
                return;
            }
        }

        // Don't finalize while concatenation is still pending.
        if self
            .pending_concat
            .get(&job_id)
            .is_some_and(|s| !s.is_empty())
        {
            debug!(
                job_id = job_id.0,
                "deferring completion — pending concatenation"
            );
            return;
        }

        // All data files downloaded — transition out of Downloading so the UI
        // reflects post-processing (verify/extract/cleanup).
        {
            let state = self.jobs.get_mut(&job_id).unwrap();
            if state.status == JobStatus::Downloading {
                state.status = JobStatus::Extracting;
                if let Err(e) = self.db.set_active_job_status(job_id, "extracting", None) {
                    error!(error = %e, "db write failed for extracting status");
                }
                self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
            }
        }

        let par2_bypassed = self.par2_bypassed.contains(&job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|f| !f.is_empty());

        if has_crc_failures && !par2_bypassed {
            if self.has_active_rar_workers(job_id) {
                info!(
                    job_id = job_id.0,
                    "deferring verify — active RAR extraction workers"
                );
                return;
            }

            let par2_set = self.par2_set(job_id).cloned();
            if let Some(par2_set) = par2_set {
                {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    if state.status == JobStatus::Extracting {
                        self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
                    }
                    state.status = JobStatus::Verifying;
                }
                if let Err(e) = self.db.set_active_job_status(job_id, "verifying", None) {
                    error!(error = %e, "db write failed for verifying status");
                }
                self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
                info!(job_id = job_id.0, "par2 verification started");
                let _ = self
                    .event_tx
                    .send(PipelineEvent::JobVerificationStarted { job_id });
                let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                });

                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                let par2_for_verify = Arc::clone(&par2_set);
                let verify_dir = working_dir.clone();

                let verify_result = tokio::task::spawn_blocking(move || {
                    let plan = weaver_par2::scan_placement(&verify_dir, &par2_for_verify)
                        .map_err(|e| format!("placement scan failed: {e}"))?;
                    if !plan.conflicts.is_empty() {
                        return Err(format!(
                            "placement scan found {} conflicting file matches",
                            plan.conflicts.len()
                        ));
                    }

                    let file_access = weaver_par2::PlacementFileAccess::from_plan(
                        verify_dir,
                        &par2_for_verify,
                        &plan,
                    );
                    Ok((
                        weaver_par2::verify_all(&par2_for_verify, &file_access),
                        plan,
                    ))
                })
                .await;

                self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

                let (mut verification, placement_plan) = match verify_result {
                    Ok(Ok(v)) => v,
                    Ok(Err(msg)) => {
                        warn!(job_id = job_id.0, error = %msg);
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Failed { error: msg.clone() };
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::JobFailed { job_id, error: msg });
                        self.record_job_history(job_id);
                        return;
                    }
                    Err(e) => {
                        let msg = format!("verification task panicked: {e}");
                        warn!(job_id = job_id.0, error = %msg);
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Failed { error: msg.clone() };
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::JobFailed { job_id, error: msg });
                        self.record_job_history(job_id);
                        return;
                    }
                };
                Self::log_placement_plan(job_id, &placement_plan);

                let (skipped_blocks, retained_suspect_blocks) =
                    self.apply_eager_delete_exclusions(job_id, &mut verification);
                if skipped_blocks > 0 {
                    info!(
                        job_id = job_id.0,
                        skipped_blocks,
                        "excluded eagerly-deleted CRC-verified volumes from damage count"
                    );
                }
                if retained_suspect_blocks > 0 {
                    info!(
                        job_id = job_id.0,
                        retained_suspect_blocks,
                        "retained suspect eagerly-deleted volumes in damage count"
                    );
                }

                self.recompute_volume_safety_from_verification(job_id, &verification);

                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity = self.total_recovery_block_capacity(job_id);
                let _ = self.event_tx.send(PipelineEvent::JobVerificationComplete {
                    job_id,
                    passed: damaged == 0,
                });

                if damaged == 0 {
                    info!(
                        job_id = job_id.0,
                        "PAR2 verification passed — no damaged slices"
                    );

                    if has_crc_failures {
                        if self.normalization_retried.contains(&job_id) {
                            let msg =
                                "clean PAR2 verification but extraction still failing after retry"
                                    .to_string();
                            warn!(job_id = job_id.0, error = %msg);
                            let state = self.jobs.get_mut(&job_id).unwrap();
                            state.status = JobStatus::Failed { error: msg.clone() };
                            let _ = self
                                .event_tx
                                .send(PipelineEvent::JobFailed { job_id, error: msg });
                            self.record_job_history(job_id);
                            return;
                        }

                        if let Err(error) = self
                            .apply_placement_plan_for_retry_or_repair(
                                job_id,
                                working_dir.clone(),
                                &placement_plan,
                            )
                            .await
                        {
                            self.fail_job(job_id, error);
                            return;
                        }

                        self.set_normalization_retried_state(job_id, true);
                        let failed_members = self
                            .failed_extractions
                            .get(&job_id)
                            .cloned()
                            .unwrap_or_default();
                        self.replace_failed_extraction_members(job_id, HashSet::new());
                        let cleared = failed_members.len();
                        self.recompute_rar_retry_frontier(job_id).await;
                        if let Some(reason) = self.invalid_rar_retry_frontier_reason(job_id) {
                            if !failed_members.is_empty() {
                                self.replace_failed_extraction_members(job_id, failed_members);
                            }
                            let msg = format!(
                                "invalid RAR retry frontier after placement correction: {reason}"
                            );
                            warn!(job_id = job_id.0, error = %msg);
                            let state = self.jobs.get_mut(&job_id).unwrap();
                            state.status = JobStatus::Failed { error: msg.clone() };
                            let _ = self
                                .event_tx
                                .send(PipelineEvent::JobFailed { job_id, error: msg });
                            self.record_job_history(job_id);
                            return;
                        }
                        info!(
                            job_id = job_id.0,
                            cleared,
                            "cleared failed extractions after authoritative verify — retrying"
                        );

                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            state.status = JobStatus::Downloading;
                        }
                        if let Err(e) = self.db.set_active_job_status(job_id, "downloading", None) {
                            error!(error = %e, "db write failed");
                        }
                        self.try_partial_extraction(job_id).await;
                        return;
                    }
                } else {
                    info!(
                        job_id = job_id.0,
                        damaged,
                        recovery_now,
                        total_recovery_capacity,
                        "PAR2 verification — damage detected"
                    );

                    if let Err(error) = self
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            working_dir.clone(),
                            &placement_plan,
                        )
                        .await
                    {
                        self.fail_job(job_id, error);
                        return;
                    }

                    if total_recovery_capacity < damaged {
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Failed {
                            error: format!(
                                "not repairable: {damaged} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        };
                        let _ = self.event_tx.send(PipelineEvent::JobFailed {
                            job_id,
                            error: format!(
                                "not repairable: {damaged} damaged, {total_recovery_capacity} recovery"
                            ),
                        });
                        self.record_job_history(job_id);
                        return;
                    }

                    if recovery_now < damaged {
                        let promoted = self.promote_recovery_targeted(job_id, damaged);
                        let targeted_total = self.recovery_blocks_available_or_targeted(job_id);
                        info!(
                            job_id = job_id.0,
                            damaged,
                            recovery_now,
                            targeted_total,
                            promoted_blocks = promoted,
                            "waiting for targeted recovery downloads before repair"
                        );
                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            state.status = JobStatus::Downloading;
                        }
                        if let Err(e) = self.db.set_active_job_status(job_id, "downloading", None) {
                            error!(error = %e, "db write failed for downloading status");
                        }
                        return;
                    }

                    {
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Repairing;
                    }
                    if let Err(e) = self.db.set_active_job_status(job_id, "repairing", None) {
                        error!(error = %e, "db write failed for repairing status");
                    }
                    self.metrics.repair_active.fetch_add(1, Ordering::Relaxed);
                    let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });

                    let par2_for_repair = Arc::clone(&par2_set);
                    let repair_dir = working_dir.clone();

                    let repair_result = tokio::task::spawn_blocking(move || {
                        let mut file_access =
                            weaver_par2::DiskFileAccess::new(repair_dir, &par2_for_repair);
                        let plan = weaver_par2::plan_repair(&par2_for_repair, &verification)
                            .map_err(|e| format!("repair planning failed: {e}"))?;
                        let slices = plan.missing_slices.len() as u32;
                        weaver_par2::execute_repair(&plan, &par2_for_repair, &mut file_access)
                            .map_err(|e| format!("repair execution failed: {e}"))?;
                        Ok(slices)
                    })
                    .await;

                    self.metrics.repair_active.fetch_sub(1, Ordering::Relaxed);

                    let repair_outcome = match repair_result {
                        Ok(Ok(slices)) => Ok(slices),
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(format!("repair task panicked: {e}")),
                    };

                    match repair_outcome {
                        Ok(slices_repaired) => {
                            info!(job_id = job_id.0, slices_repaired, "PAR2 repair complete");
                            let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                                job_id,
                                slices_repaired,
                            });

                            let cleared =
                                self.failed_extractions.get(&job_id).map_or(0, HashSet::len);
                            self.replace_failed_extraction_members(job_id, HashSet::new());
                            if cleared > 0 {
                                info!(
                                    job_id = job_id.0,
                                    cleared, "cleared failed extractions for post-repair retry"
                                );
                            }

                            if let Some(state) = self.jobs.get_mut(&job_id) {
                                state.status = JobStatus::Downloading;
                            }
                            if let Err(e) =
                                self.db.set_active_job_status(job_id, "downloading", None)
                            {
                                error!(error = %e, "db write failed for downloading status");
                            }

                            self.try_partial_extraction(job_id).await;
                            return;
                        }
                        Err(error_msg) => {
                            warn!(job_id = job_id.0, error = %error_msg, "PAR2 repair failed");
                            let Some(state) = self.jobs.get_mut(&job_id) else {
                                return;
                            };
                            state.status = JobStatus::Failed {
                                error: error_msg.clone(),
                            };
                            let _ = self.event_tx.send(PipelineEvent::RepairFailed {
                                job_id,
                                error: error_msg.clone(),
                            });
                            self.record_job_history(job_id);
                            return;
                        }
                    }
                }
            } else {
                // CRC failures but no PAR2 set — fail the job.
                let failed_members: Vec<String> = self
                    .failed_extractions
                    .get(&job_id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                let msg = format!(
                    "extraction CRC failures with no PAR2 data: {:?}",
                    failed_members
                );
                warn!(job_id = job_id.0, error = %msg);
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Failed { error: msg.clone() };
                let _ = self
                    .event_tx
                    .send(PipelineEvent::JobFailed { job_id, error: msg });
                self.record_job_history(job_id);
                return;
            }
        }

        if self.job_has_only_rar_archives(job_id) {
            self.check_rar_job_completion(job_id).await;
            return;
        }

        // Check extraction readiness.
        let readiness = {
            let state = self.jobs.get(&job_id).unwrap();
            state.assembly.extraction_readiness()
        };
        match readiness {
            ExtractionReadiness::NotApplicable => {
                if !par2_bypassed {
                    self.cleanup_par2_files(job_id).await;
                }
                // No archives — move to complete and finish.
                if let Err(error) = self.move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                    return;
                }
                let state = self.jobs.get_mut(&job_id).unwrap();
                if state.status == JobStatus::Extracting {
                    self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
                }
                state.status = JobStatus::Complete;
                if self.active_download_passes.remove(&job_id) {
                    let _ = self
                        .event_tx
                        .send(PipelineEvent::DownloadFinished { job_id });
                }
                self.clear_par2_runtime_state(job_id);
                self.clear_job_rar_runtime(job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                info!(job_id = job_id.0, "job completed (no archives)");
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Ready => {
                // Collect sets that still need extraction (some may have been
                // extracted during the partial extraction phase).
                let already_extracted = self
                    .extracted_sets
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let sets_to_extract: Vec<(String, weaver_assembly::ArchiveType)> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    state
                        .assembly
                        .archive_topologies()
                        .iter()
                        .filter(|(name, _)| !already_extracted.contains(*name))
                        .map(|(name, topo)| (name.clone(), topo.archive_type))
                        .collect()
                };

                if !sets_to_extract.is_empty() {
                    // Spawn extraction tasks in the background.
                    // handle_extraction_done will re-enter check_job_completion
                    // when each set finishes, and we'll reach the empty branch below.
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    if state.status != JobStatus::Extracting {
                        state.status = JobStatus::Extracting;
                        if let Err(e) = self.db.set_active_job_status(job_id, "extracting", None) {
                            error!(error = %e, "db write failed for extracting status");
                        }
                        self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::ExtractionReady { job_id });
                        info!(job_id = job_id.0, "extraction ready");
                    }

                    for (set_name, archive_type) in &sets_to_extract {
                        let result = match archive_type {
                            weaver_assembly::ArchiveType::SevenZip => {
                                self.extract_7z_set(job_id, set_name).await
                            }
                            weaver_assembly::ArchiveType::Rar => {
                                self.extract_rar_set(job_id, set_name).await
                            }
                            weaver_assembly::ArchiveType::Zip => {
                                self.extract_simple_archive(
                                    job_id,
                                    set_name,
                                    SimpleArchiveKind::Zip,
                                )
                                .await
                            }
                            weaver_assembly::ArchiveType::Tar => {
                                self.extract_simple_archive(
                                    job_id,
                                    set_name,
                                    SimpleArchiveKind::Tar,
                                )
                                .await
                            }
                            weaver_assembly::ArchiveType::TarGz => {
                                self.extract_simple_archive(
                                    job_id,
                                    set_name,
                                    SimpleArchiveKind::TarGz,
                                )
                                .await
                            }
                            weaver_assembly::ArchiveType::Gz => {
                                self.extract_simple_archive(job_id, set_name, SimpleArchiveKind::Gz)
                                    .await
                            }
                            weaver_assembly::ArchiveType::Split => {
                                self.extract_simple_archive(
                                    job_id,
                                    set_name,
                                    SimpleArchiveKind::Split,
                                )
                                .await
                            }
                        };
                        if let Err(e) = result {
                            warn!(job_id = job_id.0, set_name = %set_name, error = %e, "failed to start extraction");
                        }
                    }
                    // Return — extraction runs in background.
                    // handle_extraction_done will call check_job_completion again.
                    return;
                }

                // All sets extracted — finish the job.
                if self
                    .jobs
                    .get(&job_id)
                    .is_some_and(|s| s.status == JobStatus::Extracting)
                {
                    self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
                }
                info!(job_id = job_id.0, "extraction complete");
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionComplete { job_id });

                // Clean up archive source files before moving to complete.
                {
                    let state = self.jobs.get(&job_id).unwrap();
                    let cleanup_dir = state.working_dir.clone();
                    let cleanup_files: Vec<String> = state
                        .assembly
                        .files()
                        .filter(|f| {
                            matches!(
                                f.role(),
                                weaver_core::classify::FileRole::Par2 { .. }
                                    | weaver_core::classify::FileRole::RarVolume { .. }
                                    | weaver_core::classify::FileRole::SevenZipArchive
                                    | weaver_core::classify::FileRole::SevenZipSplit { .. }
                            )
                        })
                        .map(|f| f.filename().to_string())
                        .collect();
                    let mut removed = 0u32;
                    for filename in &cleanup_files {
                        let path = cleanup_dir.join(filename);
                        match tokio::fs::remove_file(&path).await {
                            Ok(()) => removed += 1,
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => {
                                warn!(
                                    file = %path.display(),
                                    error = %e,
                                    "failed to clean up source file"
                                );
                            }
                        }
                    }
                    info!(
                        job_id = job_id.0,
                        removed,
                        total = cleanup_files.len(),
                        "post-extraction cleanup complete"
                    );
                }

                // Move extracted files to complete directory.
                let was_extracting = self
                    .jobs
                    .get(&job_id)
                    .is_some_and(|state| state.status == JobStatus::Extracting);
                if let Err(error) = self.move_to_complete(job_id).await {
                    if was_extracting {
                        self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
                    }
                    self.fail_job(job_id, error);
                    return;
                }

                {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.status = JobStatus::Complete;
                }
                if self.active_download_passes.remove(&job_id) {
                    let _ = self
                        .event_tx
                        .send(PipelineEvent::DownloadFinished { job_id });
                }
                self.clear_par2_runtime_state(job_id);
                self.clear_job_rar_runtime(job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Blocked { reason } => {
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Failed {
                    error: reason.clone(),
                };
                let _ = self.event_tx.send(PipelineEvent::JobFailed {
                    job_id,
                    error: reason.clone(),
                });
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            } => {
                debug!(
                    job_id = job_id.0,
                    extractable = ?extractable,
                    waiting = ?waiting_on,
                    "partial extraction possible — waiting for remaining volumes"
                );
            }
        }
    }

    /// Extract a single RAR archive set from complete local volume files.
    pub(super) async fn extract_rar_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let (volume_paths, cached_headers, password, working_dir) = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let mut parts = std::collections::BTreeMap::new();
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
                    parts.insert(*volume_number, path);
                }
            }
            (
                parts,
                self.load_rar_snapshot(job_id, set_name),
                state.spec.password.clone(),
                state.working_dir.clone(),
            )
        };

        if let Some(set_state) = self.rar_sets.get_mut(&(job_id, set_name.to_string())) {
            set_state.active_workers = 1;
            set_state.in_flight_members.clear();
            set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
            if let Some(plan) = set_state.plan.as_mut() {
                plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
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
        let output_dir = working_dir;
        let set_name_for_result = set_name_owned.clone();
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
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
            })
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

    /// Extract a single 7z archive set. Only collects files belonging to the named set.
    pub(super) async fn extract_7z_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let (file_paths, password, working_dir) = {
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
                    parts.push((vol, state.working_dir.join(file_asm.filename())));
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            let paths: Vec<PathBuf> = parts.into_iter().map(|(_, p)| p).collect();
            (
                paths,
                state.spec.password.clone(),
                state.working_dir.clone(),
            )
        };

        let output_dir = working_dir;
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();

        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_for_channel = set_name.to_string();
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
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
                    let reader = weaver_core::split_reader::SplitFileReader::open(&file_paths)
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

    /// Extract a simple (non-RAR, non-7z) archive: ZIP, tar, tar.gz, gz, or split.
    pub(super) async fn extract_simple_archive(
        &mut self,
        job_id: JobId,
        set_name: &str,
        kind: SimpleArchiveKind,
    ) -> Result<u32, String> {
        let (file_paths, working_dir) = {
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
                    parts.push((vol, state.working_dir.join(file_asm.filename())));
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            let paths: Vec<std::path::PathBuf> = parts.into_iter().map(|(_, p)| p).collect();
            (paths, state.working_dir.clone())
        };

        let output_dir = working_dir;
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();
        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_for_channel = set_name.to_string();

        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                if file_paths.is_empty() {
                    return Err(format!("no files found for set '{set_name_owned}'"));
                }

                let extracted_members = match kind {
                    SimpleArchiveKind::Zip => extract_zip(
                        &file_paths[0],
                        &output_dir,
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
                    SimpleArchiveKind::Gz => extract_gz(
                        &file_paths[0],
                        &output_dir,
                        &event_tx,
                        job_id,
                        &set_name_owned,
                    )?,
                    SimpleArchiveKind::Split => {
                        extract_split(&file_paths, &output_dir, &event_tx, job_id, &set_name_owned)?
                    }
                };

                Ok(FullSetExtractionOutcome {
                    extracted: extracted_members,
                    failed: Vec::new(),
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
    pub(super) fn try_delete_volumes(&mut self, job_id: JobId, set_name: &str) {
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
                let Some(state) = self.jobs.get(&job_id) else {
                    return;
                };
                let path = state.working_dir.join(&filename);
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
            if let Err(error) = self.db.set_volume_status(
                job_id,
                set_name,
                volume,
                decision.ownership_eligible,
                par2_clean,
                deleted,
            ) {
                error!(
                    job_id = job_id.0,
                    set_name,
                    volume,
                    error = %error,
                    "failed to persist RAR volume eligibility"
                );
            }
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
