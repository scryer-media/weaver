//! RAR member extraction shared by disk-backed and mixed direct-store chases.

use std::collections::HashSet;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::jobs::PhaseCounters;
use crate::pipeline::FullSetExtractionOutcome;
use crate::pipeline::extraction::{
    ExtractionRoot, JobExtractionBudget, apply_rar_member_filesystem_metadata,
    apply_server_rar_limits_with_memory_limit, ensure_rar_dictionary_within_limit,
    rar_decoder_memory_bytes, rar_entry_via, validate_sanitized_rar_member_path,
};

pub(crate) struct RarChaseContext<'a> {
    pub provider: &'a dyn unrar_rs::VolumeProvider,
    pub volume_count: usize,
    pub root: &'a ExtractionRoot,
    pub output_dir: &'a Path,
    pub budget: &'a Arc<JobExtractionBudget>,
    pub password: Option<String>,
    pub counters: &'a PhaseCounters,
}

pub(crate) fn extract(
    context: RarChaseContext<'_>,
    should_extract: impl Fn(&str) -> Result<bool, String>,
) -> Result<FullSetExtractionOutcome, String> {
    let RarChaseContext {
        provider,
        volume_count,
        root,
        output_dir,
        budget,
        password,
        counters,
    } = context;
    let first = provider.get_volume(0).map_err(|error| error.to_string())?;
    let mut archive =
        unrar_rs::RarArchive::open_prefix(first, password.as_deref(), NonZeroUsize::MIN)
            .map_err(|error| error.to_string())?;
    let options = unrar_rs::ExtractOptions {
        verify: true,
        password: password.clone(),
        restore_owners: false,
    };
    let mut next_member = 0;
    let mut extracted = Vec::new();
    let mut destinations = HashSet::new();
    let mut directories: Vec<(unrar_rs::MemberInfo, std::path::PathBuf)> = Vec::new();
    // The archive keeps its dictionary between members. Keep its reservations
    // too, including while it waits for headers or the next volume.
    let mut reservations = Vec::new();
    let mut reserved = 0;
    for volume in 0..volume_count {
        let mut requested = NonZeroUsize::MIN;
        loop {
            let count = archive
                .extend_volume_prefix(
                    volume,
                    provider
                        .get_volume(volume)
                        .map_err(|error| error.to_string())?,
                    requested,
                )
                .map_err(|error| error.to_string())?;
            let ceiling =
                apply_server_rar_limits_with_memory_limit(&mut archive, budget.max_memory_bytes());
            ensure_rar_dictionary_within_limit(&archive, ceiling)
                .map_err(|error| error.to_string())?;
            let needed = rar_decoder_memory_bytes(&archive);
            if needed > reserved {
                reservations.push(budget.reserve_memory_wait(needed - reserved)?);
                reserved = needed;
            }
            while next_member < archive.len() {
                let index = next_member;
                next_member += 1;
                let info = archive
                    .member_info(index)
                    .ok_or("RAR member metadata disappeared")?;
                let name = info.name.clone();
                let relative = validate_sanitized_rar_member_path(&unrar_rs::sanitize_path(&name))?;
                if !destinations.insert(
                    relative
                        .to_string_lossy()
                        .replace('\\', "/")
                        .to_ascii_lowercase(),
                ) {
                    return Err(format!(
                        "RAR members resolve to the same destination: {name}"
                    ));
                }
                if !should_extract(&name)? {
                    if archive.is_solid() {
                        return Err(
                            "mixed direct-store chase cannot skip a solid dictionary member".into(),
                        );
                    }
                    continue;
                }
                if info.is_directory {
                    root.create_dir(&relative, budget)?;
                    directories.push((info, output_dir.join(&relative)));
                } else {
                    let mut file = root.create_file(&relative, budget)?;
                    let written = rar_entry_via(&mut archive, index, provider, &options)
                        .and_then(|entry| entry.copy_to(&mut file))
                        .map_err(|error| error.to_string())?;
                    file.flush().map_err(|error| error.to_string())?;
                    counters.total_bytes.fetch_add(written, Ordering::Relaxed);
                    counters
                        .completed_bytes
                        .fetch_add(written, Ordering::Relaxed);
                    apply_rar_member_filesystem_metadata(&info, &output_dir.join(&relative))?;
                }
                extracted.push(name);
            }
            if count < requested.get() {
                break;
            }
            requested = requested
                .checked_add(1)
                .ok_or("RAR header count overflow")?;
        }
    }
    for (info, path) in directories.into_iter().rev() {
        apply_rar_member_filesystem_metadata(&info, &path)?;
    }
    Ok(FullSetExtractionOutcome {
        extracted,
        failed: Vec::new(),
        selected_password: password,
    })
}

/// Install only the verified member manifest. Retained files from an earlier
/// process must never become output merely because they share a staging tree.
pub(crate) fn install(from: &Path, to: &Path, names: &[String]) -> Result<(), String> {
    let mut entries = Vec::with_capacity(names.len());
    let mut destinations = HashSet::new();
    for name in names {
        let relative = validate_sanitized_rar_member_path(&unrar_rs::sanitize_path(name))?;
        if !destinations.insert(relative.to_string_lossy().to_ascii_lowercase()) {
            return Err(format!("duplicate chased destination: {name}"));
        }
        ensure_directory_chain(from, relative.parent().unwrap_or(Path::new("")), false)?;
        ensure_directory_chain(to, relative.parent().unwrap_or(Path::new("")), true)?;
        let source = from.join(&relative);
        let destination = to.join(&relative);
        let metadata = std::fs::symlink_metadata(&source).map_err(|e| e.to_string())?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            return Err(format!(
                "chased member is not a regular file or directory: {name}"
            ));
        }
        match std::fs::symlink_metadata(&destination) {
            Ok(existing)
                if metadata.is_dir() && existing.is_dir() && !existing.file_type().is_symlink() => {
            }
            Ok(_) => return Err(format!("chased destination already exists: {name}")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
        entries.push((source, destination, metadata));
    }
    // All collisions are checked before installing the first file. A partial
    // installation on an I/O error is safe for the conventional retry path.
    let mut directories = Vec::new();
    for (source, destination, metadata) in entries {
        if metadata.is_dir() {
            std::fs::create_dir_all(&destination).map_err(|e| e.to_string())?;
            directories.push((destination, metadata));
            continue;
        }
        if std::fs::hard_link(&source, &destination).is_err() {
            let mut input = std::fs::File::open(&source).map_err(|e| e.to_string())?;
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&destination)
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut input, &mut output).map_err(|e| e.to_string())?;
            output
                .set_permissions(metadata.permissions())
                .map_err(|e| e.to_string())?;
            let mut times = std::fs::FileTimes::new();
            if let Ok(modified) = metadata.modified() {
                times = times.set_modified(modified);
            }
            if let Ok(accessed) = metadata.accessed() {
                times = times.set_accessed(accessed);
            }
            output.set_times(times).map_err(|e| e.to_string())?;
        }
    }
    for (destination, metadata) in directories.into_iter().rev() {
        filetime::set_file_times(
            &destination,
            filetime::FileTime::from_last_access_time(&metadata),
            filetime::FileTime::from_last_modification_time(&metadata),
        )
        .map_err(|e| e.to_string())?;
        std::fs::set_permissions(destination, metadata.permissions()).map_err(|e| e.to_string())?;
    }
    std::fs::remove_dir_all(from).map_err(|e| e.to_string())
}

fn ensure_directory_chain(root: &Path, relative: &Path, create: bool) -> Result<(), String> {
    let mut path = root.to_path_buf();
    for component in std::iter::once(None).chain(relative.components().map(Some)) {
        if let Some(component) = component {
            path.push(component);
        }
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Err(error) if create && error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&path).map_err(|e| e.to_string())?;
            }
            _ => {
                return Err(format!(
                    "unsafe chased output directory: {}",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}
