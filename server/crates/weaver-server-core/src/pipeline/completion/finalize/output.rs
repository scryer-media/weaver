use super::*;
use std::collections::HashMap;
use std::path::PathBuf;

use super::deobfuscate::{self, DeliveryNamingPlan, SrrdbInputs};
use crate::jobs::working_dir::{WORKING_DIR_MARKER, mark_weaver_owned_output_dir};
use crate::post_processing::model::PostProcessingSettings;
use crate::runtime::{file_cache, fs as runtime_fs};

/// Folds one volume's member headers into the checksum map. A member spanning
/// volumes states its checksum on the header that ends it, so later volumes
/// overwrite earlier `None`s naturally by only writing what they know.
fn record_member_crc32(by_name: &mut HashMap<String, u32>, facts: &unrar_rs::RarVolumeFacts) {
    for member in &facts.members {
        if member.is_directory {
            continue;
        }
        let Some(crc32) = member.data_crc32 else {
            continue;
        };
        let name = member
            .name
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(&member.name);
        by_name.insert(name.to_ascii_lowercase(), crc32);
    }
}

#[derive(Debug)]
struct UnacceptableExtensionMatch {
    relative_path: String,
    pattern: String,
}

/// Inspect precisely the entries the final move can publish without following
/// symlinks. A rejection occurs before the destination directory is allocated.
fn scan_delivery_sources(
    working_dir: &std::path::Path,
    staging_dir: Option<&std::path::Path>,
    settings: &PostProcessingSettings,
) -> Result<Option<UnacceptableExtensionMatch>, String> {
    if let Some(staging_dir) = staging_dir
        && let Some(rejection) = scan_delivery_root(staging_dir, &[".weaver-chunks"], settings)?
    {
        return Ok(Some(rejection));
    }
    scan_delivery_root(
        working_dir,
        &[".weaver-chunks", ".weaver-staging", WORKING_DIR_MARKER],
        settings,
    )
}

fn scan_delivery_root(
    root: &std::path::Path,
    ignored_entries: &[&str],
    settings: &PostProcessingSettings,
) -> Result<Option<UnacceptableExtensionMatch>, String> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "could not inspect delivery root {}: {error}",
                root.display()
            ));
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "could not inspect delivery root {}: {error}",
                root.display()
            )
        })?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| ignored_entries.contains(&name))
        {
            continue;
        }
        if let Some(rejection) = scan_delivery_path(root, &entry.path(), settings)? {
            return Ok(Some(rejection));
        }
    }
    Ok(None)
}

fn scan_delivery_path(
    root: &std::path::Path,
    path: &std::path::Path,
    settings: &PostProcessingSettings,
) -> Result<Option<UnacceptableExtensionMatch>, String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        format!(
            "could not inspect delivery entry {}: {error}",
            path.display()
        )
    })?;
    if metadata.is_dir() {
        let entries = std::fs::read_dir(path).map_err(|error| {
            format!(
                "could not inspect delivery directory {}: {error}",
                path.display()
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!(
                    "could not inspect delivery directory {}: {error}",
                    path.display()
                )
            })?;
            if let Some(rejection) = scan_delivery_path(root, &entry.path(), settings)? {
                return Ok(Some(rejection));
            }
        }
        return Ok(None);
    }

    let filename = path.file_name().unwrap_or_default().to_string_lossy();
    let Some(pattern) = settings.unacceptable_extension_match(&filename) else {
        return Ok(None);
    };
    let relative_path = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned();
    Ok(Some(UnacceptableExtensionMatch {
        relative_path,
        pattern: pattern.to_string(),
    }))
}

fn move_path_with_copy_fallback(
    src: &std::path::Path,
    dst: &std::path::Path,
    phase_counters: &PhaseCounters,
) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(src)?;

    if metadata.file_type().is_symlink() {
        runtime_fs::rename_no_overwrite(src, dst)?;
        return Ok(());
    }

    if metadata.is_dir() {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir(dst).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("destination already exists: {}", dst.display()),
                )
            } else {
                error
            }
        })?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            move_path_with_copy_fallback(
                &entry.path(),
                &dst.join(entry.file_name()),
                phase_counters,
            )?;
        }
        std::fs::remove_dir(src)?;
        return Ok(());
    }

    let parent_fingerprint = runtime_fs::prepare_destination_parent(dst)?;
    match file_cache::copy_large_file_with_progress(src, dst, |copied| {
        phase_counters
            .completed_bytes
            .fetch_add(copied, Ordering::Relaxed);
    }) {
        Ok(_) => {}
        Err(error) => {
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                cleanup_copy_destination_if_parent_matches(dst, &parent_fingerprint);
            }
            return Err(error);
        }
    }
    if !runtime_fs::destination_parent_matches(dst, &parent_fingerprint)? {
        return Err(std::io::Error::other(format!(
            "destination parent changed during copy: {}",
            dst.display()
        )));
    }
    std::fs::remove_file(src)?;
    Ok(())
}

fn cleanup_copy_destination_if_parent_matches(
    dst: &std::path::Path,
    parent_fingerprint: &runtime_fs::DirectoryFingerprint,
) {
    if runtime_fs::destination_parent_matches(dst, parent_fingerprint).unwrap_or(false) {
        let _ = std::fs::remove_file(dst);
    }
}

fn rename_path_for_publication(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(src)?;
    if !metadata.is_dir() {
        return runtime_fs::rename_no_overwrite(src, dst);
    }

    match std::fs::symlink_metadata(dst) {
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dst.display()),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    std::fs::rename(src, dst)
}

fn move_path_with_safe_rename_or_copy_fallback(
    src: &std::path::Path,
    dst: &std::path::Path,
    phase_counters: Arc<PhaseCounters>,
) -> Result<(), (std::io::Error, std::io::Error)> {
    move_path_with_safe_rename_or_copy_fallback_using(
        src,
        dst,
        phase_counters,
        rename_path_for_publication,
        move_path_with_copy_fallback,
    )
}

fn move_path_with_safe_rename_or_copy_fallback_using<R, C>(
    src: &std::path::Path,
    dst: &std::path::Path,
    phase_counters: Arc<PhaseCounters>,
    rename: R,
    copy: C,
) -> Result<(), (std::io::Error, std::io::Error)>
where
    R: FnOnce(&std::path::Path, &std::path::Path) -> std::io::Result<()>,
    C: FnOnce(&std::path::Path, &std::path::Path, &PhaseCounters) -> std::io::Result<()>,
{
    let path_bytes = path_regular_file_bytes(src).unwrap_or(0);
    match rename(src, dst) {
        Ok(()) => {
            if path_bytes > 0 {
                phase_counters
                    .completed_bytes
                    .fetch_add(path_bytes, Ordering::Relaxed);
            }
            Ok(())
        }
        Err(rename_err) if rename_err.kind() == std::io::ErrorKind::CrossesDevices => {
            copy(src, dst, &phase_counters).map_err(|copy_err| (rename_err, copy_err))
        }
        Err(rename_err) => Err((
            rename_err,
            std::io::Error::other("copy fallback not attempted for a non-cross-device error"),
        )),
    }
}

fn path_regular_file_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }
    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(path_regular_file_bytes(&entry.path())?);
    }
    Ok(total)
}

async fn run_move_to_complete(
    job_id: JobId,
    working_dir: PathBuf,
    staging_dir: Option<PathBuf>,
    dest: PathBuf,
    phase_counters: Arc<PhaseCounters>,
    naming: Option<DeliveryNamingPlan>,
) -> Result<MoveToCompleteResult, String> {
    // Every cached disk write handle under either of the job's roots must be
    // closed before its files are renamed or moved. The staging root is not
    // only extraction output any more: direct-store writes member payload
    // straight into it, through the same owner pool.
    crate::pipeline::close_cached_write_handles_under(&working_dir).await;
    if let Some(staging) = staging_dir.as_deref() {
        crate::pipeline::close_cached_write_handles_under(staging).await;
    }

    // Verify at least one source directory exists before creating
    // the destination, so a missing source doesn't leave behind an
    // empty complete dir.
    let staging_exists = staging_dir.as_ref().is_some_and(|s| s.exists());
    let working_exists = working_dir.exists();
    if !staging_exists && !working_exists {
        return Err(format!(
            "failed to read working directory {} for move: No such file or directory (os error 2)",
            working_dir.display()
        ));
    }

    let total_bytes = {
        let working_dir = working_dir.clone();
        let staging_dir = staging_dir.clone();
        tokio::task::spawn_blocking(move || {
            move_sources_regular_file_bytes(&working_dir, staging_dir.as_deref())
        })
        .await
        .unwrap_or(0)
    };
    phase_counters
        .total_bytes
        .store(total_bytes, Ordering::Relaxed);

    if let Err(error) = tokio::fs::create_dir_all(&dest).await {
        return Err(format!(
            "failed to create complete directory {}: {error}",
            dest.display()
        ));
    }

    let mut moved = 0u32;
    let mut failures = Vec::new();

    if let Some(ref staging) = staging_dir
        && let Ok(mut entries) = tokio::fs::read_dir(staging).await
    {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name();
            if file_name == ".weaver-chunks" {
                let src = entry.path();
                if let Err(error) = tokio::fs::remove_dir_all(&src).await
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(
                        job_id = job_id.0,
                        path = %src.display(),
                        error = %error,
                        "failed to remove chunk workspace during final move"
                    );
                }
                continue;
            }
            let src = entry.path();
            let dst = dest.join(&file_name);
            let src_fb = src.clone();
            let dst_fb = dst.clone();
            let counters = Arc::clone(&phase_counters);
            match tokio::task::spawn_blocking(move || {
                move_path_with_safe_rename_or_copy_fallback(&src_fb, &dst_fb, counters)
            })
            .await
            {
                Ok(Ok(())) => {
                    moved += 1;
                }
                Ok(Err((rename_err, copy_err))) => {
                    failures.push(format!(
                        "{}: rename failed: {}; fallback failed: {}",
                        file_name.to_string_lossy(),
                        rename_err,
                        copy_err
                    ));
                }
                Err(join_err) => {
                    failures.push(format!(
                        "{}: move task failed: {}",
                        file_name.to_string_lossy(),
                        join_err
                    ));
                }
            }
        }
    }

    if let Ok(mut entries) = tokio::fs::read_dir(&working_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name();
            if file_name == ".weaver-chunks"
                || file_name == ".weaver-staging"
                || file_name == WORKING_DIR_MARKER
            {
                continue;
            }
            let src = entry.path();
            let dst = dest.join(&file_name);
            if dst.exists() && !runtime_fs::paths_equivalent_for_placement(&src, &dst) {
                continue;
            }
            let src_fb = src.clone();
            let dst_fb = dst.clone();
            let counters = Arc::clone(&phase_counters);
            match tokio::task::spawn_blocking(move || {
                move_path_with_safe_rename_or_copy_fallback(&src_fb, &dst_fb, counters)
            })
            .await
            {
                Ok(Ok(())) => {
                    moved += 1;
                }
                Ok(Err((rename_err, copy_err))) => {
                    failures.push(format!(
                        "{}: rename failed: {}; fallback failed: {}",
                        file_name.to_string_lossy(),
                        rename_err,
                        copy_err
                    ));
                }
                Err(join_err) => {
                    failures.push(format!(
                        "{}: move task failed: {}",
                        file_name.to_string_lossy(),
                        join_err
                    ));
                }
            }
        }
    }

    if !failures.is_empty() {
        for failure in &failures {
            warn!(job_id = job_id.0, error = %failure, "failed to move entry to complete directory");
        }
        return Err(format!(
            "failed to move {} entr{} to complete directory: {}",
            failures.len(),
            if failures.len() == 1 { "y" } else { "ies" },
            failures[0]
        ));
    }

    // Both delivery routes have landed in `dest` and nothing else will be added
    // to it, so this is the first and only moment the delivered set exists as
    // one directory. Renaming here is still a same-directory rename, and it
    // finishes before the move reports done — everything downstream of the
    // move sees only the final names.
    let renamed_members = match naming {
        Some(naming) => deobfuscate::rename_obfuscated_members(job_id, &dest, &naming).await,
        None => 0,
    };

    let output_dir = dest.clone();
    match tokio::task::spawn_blocking(move || mark_weaver_owned_output_dir(&output_dir)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(
            job_id = job_id.0,
            dir = %dest.display(),
            error = %error,
            "could not mark complete output directory as Weaver-owned; recursive history cleanup will refuse it"
        ),
        Err(error) => warn!(
            job_id = job_id.0,
            dir = %dest.display(),
            error = %error,
            "output ownership marker worker failed; recursive history cleanup will refuse the directory"
        ),
    }

    if let Some(ref staging) = staging_dir
        && let Err(error) = tokio::fs::remove_dir_all(staging).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            job_id = job_id.0,
            dir = %staging.display(),
            error = %error,
            "failed to remove staging directory after move"
        );
    }

    let marker_path = working_dir.join(WORKING_DIR_MARKER);
    if let Err(error) = tokio::fs::remove_file(&marker_path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            job_id = job_id.0,
            path = %marker_path.display(),
            error = %error,
            "failed to remove working directory marker during final move"
        );
    }

    if let Err(error) = tokio::fs::remove_dir(&working_dir).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            job_id = job_id.0,
            dir = %working_dir.display(),
            error = %error,
            "failed to remove intermediate directory after move"
        );
    }

    Ok(MoveToCompleteResult {
        moved_entries: moved,
        renamed_members,
    })
}

fn move_sources_regular_file_bytes(
    working_dir: &std::path::Path,
    staging_dir: Option<&std::path::Path>,
) -> u64 {
    let mut total = 0u64;
    if let Some(staging) = staging_dir
        && let Ok(entries) = std::fs::read_dir(staging)
    {
        for entry in entries.flatten() {
            if entry.file_name() == ".weaver-chunks" {
                continue;
            }
            total = total.saturating_add(path_regular_file_bytes(&entry.path()).unwrap_or(0));
        }
    }
    if let Ok(entries) = std::fs::read_dir(working_dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            if file_name == ".weaver-chunks"
                || file_name == ".weaver-staging"
                || file_name == WORKING_DIR_MARKER
            {
                continue;
            }
            total = total.saturating_add(path_regular_file_bytes(&entry.path()).unwrap_or(0));
        }
    }
    total
}

fn complete_parent_for_category(
    complete_dir: &std::path::Path,
    categories: &[crate::categories::CategoryConfig],
    category: Option<&str>,
) -> Result<PathBuf, String> {
    crate::categories::completion_parent(complete_dir, categories, category)
}

#[cfg(test)]
mod category_destination_tests {
    use super::*;

    fn category(name: &str, dest_dir: Option<&str>) -> crate::categories::CategoryConfig {
        crate::categories::CategoryConfig {
            id: 1,
            name: name.to_string(),
            dest_dir: dest_dir.map(str::to_string),
            aliases: String::new(),
        }
    }

    #[test]
    fn safe_categories_and_collision_suffixes_remain_beneath_complete_dir() {
        let complete = std::path::Path::new("/downloads/complete");
        let parent = complete_parent_for_category(complete, &[], Some("tv-hd")).unwrap();
        assert_eq!(parent, complete.join("tv-hd"));

        let collision = parent.join(weaver_model::files::path_component_with_suffix(
            "release", ".#42.1",
        ));
        assert!(collision.starts_with(complete));
    }

    #[test]
    fn configured_destination_override_remains_trusted_admin_input() {
        let complete = std::path::Path::new("/downloads/complete");
        let categories = vec![category("custom/name", Some("/mnt/admin-selected"))];

        assert_eq!(
            complete_parent_for_category(complete, &categories, Some("custom/name")).unwrap(),
            PathBuf::from("/mnt/admin-selected")
        );
    }

    #[test]
    fn malicious_or_legacy_unsafe_categories_fail_before_a_destination_is_returned() {
        let complete = std::path::Path::new("/downloads/complete");
        for unsafe_category in ["/tmp", "../../outside", "nested/path", "C:\\outside"] {
            assert!(
                complete_parent_for_category(complete, &[], Some(unsafe_category)).is_err(),
                "accepted {unsafe_category:?}"
            );
        }

        let legacy = vec![category("legacy/unsafe", None)];
        assert!(complete_parent_for_category(complete, &legacy, Some("legacy/unsafe")).is_err());
    }
}

impl Pipeline {
    fn complete_destination_is_reserved(&self, job_id: JobId, candidate: &std::path::Path) -> bool {
        self.reserved_complete_destinations
            .iter()
            .any(|(reserved_job_id, reserved_path)| {
                *reserved_job_id != job_id && reserved_path == candidate
            })
    }

    async fn compute_complete_destination(
        &self,
        job_id: JobId,
        job_name: &str,
        category: Option<&str>,
    ) -> Result<PathBuf, String> {
        let dir_name = crate::jobs::working_dir::sanitize_dirname(job_name);
        let parent = {
            let cfg = self.config.read().await;
            complete_parent_for_category(&self.complete_dir, &cfg.categories, category)?
        };
        let base_dest = parent.join(&dir_name);

        if !base_dest.exists() && !self.complete_destination_is_reserved(job_id, &base_dest) {
            return Ok(base_dest);
        }

        let parent = base_dest
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let suffixed = parent.join(weaver_model::files::path_component_with_suffix(
            &dir_name,
            &format!(".#{}", job_id.0),
        ));
        if !suffixed.exists() && !self.complete_destination_is_reserved(job_id, &suffixed) {
            return Ok(suffixed);
        }

        let mut attempt = 1u32;
        loop {
            let candidate = parent.join(weaver_model::files::path_component_with_suffix(
                &dir_name,
                &format!(".#{}.{}", job_id.0, attempt),
            ));
            if !candidate.exists() && !self.complete_destination_is_reserved(job_id, &candidate) {
                return Ok(candidate);
            }
            attempt += 1;
        }
    }

    /// Resolves everything the delivery rename pass needs, on this task, before
    /// the move worker is spawned.
    ///
    /// `None` disables the pass. The worker gets an owned plan rather than a
    /// handle to pipeline state so the pass cannot reach back into the
    /// orchestrator, and so the outbound lookup — the one step that can take
    /// seconds — never runs on the orchestrator's own task.
    async fn delivery_naming_plan(
        &self,
        job_id: JobId,
        job_name: &str,
    ) -> Option<DeliveryNamingPlan> {
        let (enabled, srrdb_from_config) = {
            let cfg = self.config.read().await;
            (
                cfg.deobfuscate_delivered_members(),
                cfg.enable_srrdb_lookup(),
            )
        };
        if !enabled {
            return None;
        }
        // The checksum map is only ever read by the lookup, so an operator who
        // never opted in never pays for gathering it.
        let srrdb = srrdb_from_config.then(|| SrrdbInputs {
            base_url: deobfuscate::SRRDB_API_BASE.to_string(),
            crc32_by_member_name: HashMap::new(),
        });
        let srrdb = match srrdb {
            Some(mut inputs) => {
                inputs.crc32_by_member_name = self.member_crc32_by_name(job_id).await;
                Some(inputs)
            }
            None => None,
        };

        Some(DeliveryNamingPlan {
            job_display_name: job_name.to_string(),
            srrdb,
        })
    }

    /// The CRC32 each archive header stated for its member, keyed by the
    /// member's filename lowercased.
    ///
    /// Read from two places because the two delivery routes keep the same facts
    /// in different homes: extraction keeps a set's parsed headers in memory
    /// until the job leaves the pipeline, while direct-store only ever writes
    /// them to the durable facts table. Reading both makes the map route-blind.
    /// A member the headers never stated a checksum for is simply absent, and
    /// the lookup falls back to the job name for it.
    async fn member_crc32_by_name(&self, job_id: JobId) -> HashMap<String, u32> {
        let mut by_name = HashMap::new();
        for ((set_job_id, _), state) in &self.rar_sets {
            if *set_job_id != job_id {
                continue;
            }
            for facts in state.facts.values() {
                record_member_crc32(&mut by_name, facts);
            }
        }

        let persisted = self
            .db_blocking(move |db| db.load_all_rar_volume_facts(job_id))
            .await;
        match persisted {
            Ok(sets) => {
                for (_, volumes) in sets {
                    for (_, blob) in volumes {
                        if let Ok(facts) = rmp_serde::from_slice::<unrar_rs::RarVolumeFacts>(&blob)
                        {
                            record_member_crc32(&mut by_name, &facts);
                        }
                    }
                }
            }
            Err(error) => debug!(
                job_id = job_id.0,
                error = %error,
                "could not read persisted volume facts for the release-index lookup"
            ),
        }
        by_name
    }

    /// Move extracted/completed files from the intermediate working directory
    /// to the complete directory, organized by category.
    ///
    /// Layout: `{complete_dir}/[{category}/]{job_name}/`
    /// On collision, appends `.#<job_id>` (and a numeric suffix if needed).
    ///
    /// Uses rename() for same-filesystem moves, falls back to copy+delete for cross-FS.
    pub(crate) async fn start_move_to_complete(&mut self, job_id: JobId) -> Result<(), String> {
        if self.inflight_moves.contains(&job_id) {
            return Ok(());
        }

        // The last gate at which every settlement fact is still in hand, and
        // the last at which refusing costs nothing: nothing has moved yet. The
        // census rebuilds the terminal record from what claimed each payload
        // file, and refuses the delivery outright when a file was never
        // delivered and nothing accounts for it.
        self.reconcile_terminal_delivery(job_id)?;

        // Terminal transition: the job is leaving the verification question
        // behind. With no recovery set there was never a verdict to be had, so
        // attribute it rather than leaving the job out of
        // `weaver_verifications_total` entirely. No-op when a pass ruled.
        self.note_job_unverifiable_if_no_par2_set(job_id);

        let (working_dir, staging_dir, job_name, category) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Err(format!("job {} not found for final move", job_id.0));
            };
            (
                state.working_dir.clone(),
                state.staging_dir.clone(),
                state.spec.name.clone(),
                state.spec.category.clone(),
            )
        };

        if let Some(staging) = staging_dir.as_deref() {
            let budget = self.extraction_budget(job_id, staging)?;
            ExtractionRoot::open(staging)?.scan_no_links(&budget)?;
        }

        let settings = self
            .db_blocking(|db| db.post_processing_settings())
            .await
            .map_err(|error| format!("could not load unacceptable extension policy: {error}"))?;
        if !settings.unacceptable_extensions.is_empty() {
            let working_dir_for_scan = working_dir.clone();
            let staging_dir_for_scan = staging_dir.clone();
            let rejection = tokio::task::spawn_blocking(move || {
                scan_delivery_sources(
                    &working_dir_for_scan,
                    staging_dir_for_scan.as_deref(),
                    &settings,
                )
            })
            .await
            .map_err(|error| format!("unacceptable extension scan worker failed: {error}"))??;
            if let Some(rejection) = rejection {
                self.reject_unacceptable_extension(
                    job_id,
                    format!(
                        "unacceptable extension '{}' matched '{}' before publication",
                        rejection.pattern, rejection.relative_path
                    ),
                );
                return Ok(());
            }
        }

        self.phase_end(job_id, JobPhase::Extracting);
        self.phase_end(job_id, JobPhase::Repairing);
        let phase_counters = self.phase_begin(job_id, JobPhase::Moving, None);
        self.transition_postprocessing_status(job_id, JobStatus::Moving, Some("moving"));

        let dest = self
            .compute_complete_destination(job_id, &job_name, category.as_deref())
            .await?;
        self.reserved_complete_destinations
            .insert(job_id, dest.clone());
        self.inflight_moves.insert(job_id);

        let _ = self
            .event_tx
            .send(PipelineEvent::MoveToCompleteStarted { job_id });
        self.publish_snapshot();

        let naming = self.delivery_naming_plan(job_id, &job_name).await;

        let move_done_tx = self.move_done_tx.clone();
        info!(
            job_id = job_id.0,
            dest = %dest.display(),
            "starting final move"
        );
        tokio::spawn(async move {
            let move_started = Instant::now();
            let result = run_move_to_complete(
                job_id,
                working_dir,
                staging_dir,
                dest.clone(),
                phase_counters,
                naming,
            )
            .await;
            match &result {
                Ok(outcome) => info!(
                    job_id = job_id.0,
                    moved = outcome.moved_entries,
                    dest = %dest.display(),
                    elapsed_ms = move_started.elapsed().as_millis(),
                    "final move finished"
                ),
                Err(error) => warn!(
                    job_id = job_id.0,
                    dest = %dest.display(),
                    elapsed_ms = move_started.elapsed().as_millis(),
                    error = %error,
                    "final move failed"
                ),
            }
            let _ = move_done_tx
                .send(MoveToCompleteDone {
                    job_id,
                    dest,
                    result,
                })
                .await;
        });
        Ok(())
    }

    pub(crate) async fn handle_move_to_complete_done(&mut self, done: MoveToCompleteDone) {
        let MoveToCompleteDone {
            job_id,
            dest,
            result,
        } = done;
        self.phase_end(job_id, JobPhase::Moving);
        self.inflight_moves.remove(&job_id);
        self.reserved_complete_destinations.remove(&job_id);

        match result {
            Ok(outcome) => {
                if outcome.renamed_members > 0 {
                    self.metrics
                        .deobfuscated_members_renamed
                        .fetch_add(u64::from(outcome.renamed_members), Ordering::Relaxed);
                }
                let Some(state) = self.jobs.get_mut(&job_id) else {
                    warn!(
                        job_id = job_id.0,
                        dest = %dest.display(),
                        "final move finished after job runtime was removed"
                    );
                    return;
                };
                state.working_dir = dest.clone();
                state.staging_dir = None;

                let _ = self
                    .event_tx
                    .send(PipelineEvent::MoveToCompleteFinished { job_id });
                info!(
                    job_id = job_id.0,
                    moved = outcome.moved_entries,
                    dest = %dest.display(),
                    "built-in pipeline completed final move"
                );
                self.start_terminal_post_processing(job_id);
            }
            Err(error) => self.fail_job(job_id, error),
        }
    }

    fn start_terminal_post_processing(&mut self, job_id: JobId) {
        self.start_terminal_post_processing_with_outcome(
            job_id,
            crate::post_processing::model::PipelineOutcome::Succeeded,
            None,
        );
    }

    /// Resolve the job's script list and, when it is non-empty, run it.
    ///
    /// Resolution happens here rather than at submission time: the list a job
    /// runs is the one configured when it finishes, which is what both oracles
    /// do and what removes the "edited while queued" race entirely.
    pub(crate) fn start_terminal_post_processing_with_outcome(
        &mut self,
        job_id: JobId,
        pipeline_outcome: crate::post_processing::model::PipelineOutcome,
        primary_failure: Option<String>,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let category = state.spec.category.clone();
        let metadata = state.spec.metadata.clone();
        let admission = match self
            .terminal_post_processing_executor
            .admit_job_scripts(category.as_deref(), &metadata)
        {
            Ok(Some(admission)) => admission,
            Ok(None) => {
                match primary_failure {
                    Some(failure) => {
                        self.finalize_failed_job_after_terminal_post_processing(job_id, failure);
                    }
                    None => self.complete_job_after_terminal_post_processing(job_id),
                }
                return;
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "could not admit post-processing scripts; skipping scripts"
                );
                match primary_failure {
                    Some(failure) => {
                        self.finalize_failed_job_after_terminal_post_processing(job_id, failure);
                    }
                    None => self.complete_job_after_terminal_post_processing(job_id),
                }
                return;
            }
        };
        if !admission.has_enabled_entries() {
            match primary_failure {
                Some(failure) => {
                    self.finalize_failed_job_after_terminal_post_processing(job_id, failure);
                }
                None => self.complete_job_after_terminal_post_processing(job_id),
            }
            return;
        }
        if !self.inflight_terminal_post_processing.insert(job_id) {
            return;
        }
        // Low-frequency: at most one terminal post-processing pass per job.
        self.note_stage_started(
            job_id,
            crate::operations::instrumentation::JobStageKind::PostProcess,
        );
        self.launch_terminal_post_processing_run(
            job_id,
            admission,
            pipeline_outcome,
            primary_failure,
        );
    }

    fn launch_terminal_post_processing_run(
        &mut self,
        job_id: JobId,
        admission: crate::post_processing::executor::PostProcessingJobAdmission,
        pipeline_outcome: crate::post_processing::model::PipelineOutcome,
        primary_failure: Option<String>,
    ) {
        // Scripts read health to decide whether the download is worth acting
        // on, so they must be handed the settled figure — the one the terminal
        // record will carry — and not the live wire counter the settlement has
        // already answered.
        let settled_health = self
            .jobs
            .get(&job_id)
            .map(|state| state.spec.total_bytes)
            .map(|total_bytes| self.terminal_record_figures(job_id, total_bytes).1);
        let Some(state) = self.jobs.get(&job_id) else {
            self.inflight_terminal_post_processing.remove(&job_id);
            return;
        };
        let pipeline_failure_stage = match &pipeline_outcome {
            crate::post_processing::model::PipelineOutcome::Failed { stage, .. } => Some(*stage),
            crate::post_processing::model::PipelineOutcome::Succeeded => None,
        };
        let par_status = if matches!(
            pipeline_failure_stage,
            Some(
                crate::post_processing::model::PipelineFailureStage::Verify
                    | crate::post_processing::model::PipelineFailureStage::Repair
            )
        ) {
            1
        } else if self.par2_verified.contains(&job_id) {
            2
        } else {
            0
        };
        let unpack_status = if matches!(
            pipeline_failure_stage,
            Some(crate::post_processing::model::PipelineFailureStage::Extract)
        ) {
            1
        } else if self
            .extracted_archives
            .get(&job_id)
            .is_some_and(|archives| !archives.is_empty())
        {
            2
        } else {
            0
        };
        let (data_dir, intermediate_dir, complete_dir) = self
            .config
            .try_read()
            .ok()
            .map(|config| {
                (
                    std::path::PathBuf::from(&config.data_dir),
                    std::path::PathBuf::from(config.intermediate_dir()),
                    std::path::PathBuf::from(config.complete_dir()),
                )
            })
            .unwrap_or_else(|| {
                (
                    self.intermediate_dir
                        .parent()
                        .map(std::path::PathBuf::from)
                        .unwrap_or_default(),
                    self.intermediate_dir.clone(),
                    self.complete_dir.clone(),
                )
            });
        let failure_message = match &pipeline_outcome {
            crate::post_processing::model::PipelineOutcome::Failed { message, .. } => {
                Some(message.clone())
            }
            crate::post_processing::model::PipelineOutcome::Succeeded => None,
        };
        let context = crate::post_processing::runner::JobExecutionContext {
            job_id: job_id.0,
            name: state.spec.name.clone(),
            nzb_filename: format!("{}.nzb", state.spec.name),
            category: state.spec.category.clone(),
            group: None,
            source_url: None,
            working_directory: state.working_dir.clone(),
            final_directory: state.working_dir.clone(),
            pipeline_outcome,
            par_status,
            unpack_status,
            compatibility: crate::post_processing::runner::CompatibilityFacts {
                total_bytes: state.spec.total_bytes,
                downloaded_bytes: state.downloaded_bytes,
                health_milli: settled_health
                    .unwrap_or_else(|| health_milli(state.spec.total_bytes, state.failed_bytes)),
                critical_health_milli: Self::critical_health_milli(
                    state.spec.total_bytes,
                    state.par2_bytes,
                ),
                password: state.spec.password.clone(),
                failure_message,
                data_dir: Some(data_dir),
                intermediate_dir: Some(intermediate_dir),
                complete_dir: Some(complete_dir),
                temp_dir: Some(std::env::temp_dir()),
                app_dir: std::env::current_exe()
                    .ok()
                    .and_then(|path| path.parent().map(std::path::PathBuf::from)),
                previous_script_status: Default::default(),
            },
        };
        self.transition_postprocessing_status(
            job_id,
            JobStatus::QueuedPostProcessing,
            Some("queued for post-processing scripts"),
        );
        self.publish_snapshot();
        let (cancellation_tx, cancellation_rx) = tokio::sync::watch::channel(false);
        self.terminal_post_processing_cancellations
            .insert(job_id, cancellation_tx);
        let executor = self.terminal_post_processing_executor.clone();
        let done_tx = self.terminal_post_processing_done_tx.clone();
        tokio::spawn(async move {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let execution = executor.execute_admitted_job(
                job_id.0,
                admission,
                context,
                Some(cancellation_rx),
                Some(started_tx),
            );
            tokio::pin!(execution);
            tokio::pin!(started_rx);
            let result = tokio::select! {
                result = &mut execution => result,
                started = &mut started_rx => {
                    if started.is_ok() {
                        let _ = done_tx
                            .send(TerminalPostProcessingEvent::Started(job_id))
                            .await;
                    }
                    execution.await
                }
            };
            let _ = done_tx
                .send(TerminalPostProcessingEvent::Done(
                    TerminalPostProcessingDone {
                        job_id,
                        primary_failure,
                        result,
                    },
                ))
                .await;
        });
    }

    /// Finish a job that was restored while it sat in post-processing.
    ///
    /// The startup recovery scan already stamped `interrupted` on every such
    /// job, so nothing is rerun here: a script that was mid-flight when weaver
    /// stopped has unknown side effects, and running it again is worse than
    /// reporting that it was interrupted.
    pub(crate) fn recover_restored_terminal_post_processing(&mut self, job_id: JobId) -> bool {
        let is_terminal_post_processing = self.jobs.get(&job_id).is_some_and(|state| {
            matches!(
                state.status,
                JobStatus::QueuedPostProcessing | JobStatus::PostProcessing
            )
        });
        if !is_terminal_post_processing {
            return false;
        }
        self.remove_pending_completion_check(job_id);
        let summary = self
            .db
            .job_post_processing_summary(job_id.0)
            .unwrap_or_default()
            .unwrap_or(crate::post_processing::model::PostProcessingSummary::Interrupted);
        if summary == crate::post_processing::model::PostProcessingSummary::NotRun {
            // Nothing had started yet, so the job is safe to run from the top —
            // the same guarantee the durable queue used to provide.
            info!(
                job_id = job_id.0,
                "resuming post-processing for a restored job whose scripts never started"
            );
            self.start_terminal_post_processing(job_id);
            return true;
        }
        let results = self
            .db
            .job_post_processing_results(job_id.0)
            .unwrap_or_default();
        let primary_failure = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.failure_error.clone());
        info!(
            job_id = job_id.0,
            summary = summary.as_str(),
            "finalizing restored post-processing without rerunning scripts"
        );
        self.handle_terminal_post_processing_done(TerminalPostProcessingDone {
            job_id,
            primary_failure,
            result: Ok(crate::post_processing::executor::JobPostProcessingReport {
                summary,
                results,
            }),
        });
        true
    }

    pub(crate) fn handle_terminal_post_processing_started(&mut self, job_id: JobId) {
        if !self.inflight_terminal_post_processing.contains(&job_id)
            || !self.jobs.contains_key(&job_id)
        {
            return;
        }
        self.transition_postprocessing_status(
            job_id,
            JobStatus::PostProcessing,
            Some("running post-processing scripts"),
        );
        self.publish_snapshot();
    }

    pub(crate) fn handle_terminal_post_processing_done(
        &mut self,
        done: TerminalPostProcessingDone,
    ) {
        self.inflight_terminal_post_processing.remove(&done.job_id);
        // Low-frequency: closes the timer armed when the run was launched.
        self.note_stage_finished(
            done.job_id,
            crate::operations::instrumentation::JobStageKind::PostProcess,
        );
        self.terminal_post_processing_cancellations
            .remove(&done.job_id);
        // User cancellation archives and removes the runtime state before the
        // interrupted script reports completion. Its delayed result must not
        // recreate or overwrite that cancelled job history.
        if !self.jobs.contains_key(&done.job_id) {
            return;
        }
        if let Some(primary_failure) = done.primary_failure {
            match &done.result {
                Ok(report) => info!(
                    job_id = done.job_id.0,
                    summary = ?report.summary,
                    "failure post-processing finished; preserving primary pipeline failure"
                ),
                Err(error) => warn!(
                    job_id = done.job_id.0,
                    error = %error,
                    "failure post-processing could not complete; preserving primary pipeline failure"
                ),
            }
            self.finalize_failed_job_after_terminal_post_processing(done.job_id, primary_failure);
            return;
        }
        match done.result {
            Ok(report)
                if matches!(
                    report.summary,
                    crate::post_processing::model::PostProcessingSummary::Succeeded
                        | crate::post_processing::model::PostProcessingSummary::Warning
                        | crate::post_processing::model::PostProcessingSummary::NotRun
                ) =>
            {
                self.complete_job_after_terminal_post_processing(done.job_id);
            }
            Ok(report) => self.finalize_failed_job_after_terminal_post_processing(
                done.job_id,
                format!(
                    "post-processing scripts ended with {}",
                    report.summary.as_str()
                ),
            ),
            Err(error) => self.finalize_failed_job_after_terminal_post_processing(
                done.job_id,
                format!("post-processing scripts failed: {error}"),
            ),
        }
    }

    pub(in crate::pipeline) fn complete_job_after_terminal_post_processing(
        &mut self,
        job_id: JobId,
    ) {
        self.transition_completed_runtime(job_id);
        if self.active_download_passes.remove(&job_id) {
            self.phase_end(job_id, JobPhase::Downloading);
            let _ = self.event_tx.send(PipelineEvent::DownloadFinished {
                job_id,
                finalization_pending: false,
            });
        }
        self.jobs_finalizing_download.remove(&job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_rar_runtime(job_id);
        self.job_order.retain(|id| *id != job_id);
        info!(
            job_id = job_id.0,
            "job completed after terminal post-processing"
        );
        self.record_job_history(job_id, Some(PipelineEvent::JobCompleted { job_id }));
        self.publish_snapshot();
    }

    pub(in crate::pipeline) fn resolve_job_input_path(
        &self,
        job_id: JobId,
        relative_path: &str,
    ) -> Option<PathBuf> {
        let state = self.jobs.get(&job_id)?;
        let working_path = state.working_dir.join(relative_path);
        if working_path.exists() {
            return Some(working_path);
        }

        if let Some(staging_dir) = state.staging_dir.as_ref() {
            let staging_path = staging_dir.join(relative_path);
            if staging_path.exists() {
                return Some(staging_path);
            }
        }

        Some(working_path)
    }
}

#[cfg(test)]
mod tests;
