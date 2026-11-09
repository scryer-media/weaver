// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::deobfuscate::{self, DeliveryNamingPlan, SrrdbInputs};
use crate::jobs::working_dir::{WORKING_DIR_MARKER, mark_weaver_owned_output_dir};
use crate::post_processing::model::PostProcessingSettings;
use crate::runtime::{file_cache, fs as runtime_fs};

#[derive(Debug)]
struct UnacceptableExtensionMatch {
    // Relative to the delivery, `/`-separated: the path the file would have
    // been published under.
    relative_path: String,
}

#[derive(Debug, Default)]
struct DeliverySourceValidation {
    total_bytes: u64,
    root_entry_bytes: HashMap<PathBuf, u64>,
}

// Each delivered file's CRC32 as its object's headers state it, by its path
// in lowercase. A path two members name with different checksums names
// none, since either could be the file there.
pub(in crate::pipeline) fn member_crc32_by_path(
    files: &[crate::pipeline::graph::status::Delivered],
) -> HashMap<String, u32> {
    let mut by_path = HashMap::new();
    let mut ambiguous = HashSet::new();
    for file in files {
        let Some(crc32) = file.crc32 else {
            continue;
        };
        let key = file.name.to_ascii_lowercase();
        if ambiguous.contains(&key) {
            continue;
        }
        match by_path.get(&key).copied() {
            None => {
                by_path.insert(key, crc32);
            }
            Some(existing) if existing == crc32 => {}
            Some(_) => {
                by_path.remove(&key);
                ambiguous.insert(key);
            }
        }
    }
    by_path
}

enum DeliveryPolicySource {
    Persisted(crate::Database),
    #[cfg(test)]
    Fixed(PostProcessingSettings),
}

// Inspect exactly the entries the final move can publish, without following
// links. This one iterative walk both enforces the extension policy and
// supplies the byte totals used by move progress.
fn validate_delivery_sources(
    working_dir: &Path,
    staging_dir: Option<&Path>,
    settings: &PostProcessingSettings,
    left_behind: &HashSet<PathBuf>,
) -> Result<DeliverySourceValidation, String> {
    let mut validation = DeliverySourceValidation::default();
    let mut source_exists = false;
    if let Some(staging_dir) = staging_dir {
        source_exists |= scan_delivery_root(
            staging_dir,
            "staging",
            &[".weaver-chunks", WORKING_DIR_MARKER],
            &HashSet::new(),
            settings,
            &mut validation,
        )?;
    }
    source_exists |= scan_delivery_root(
        working_dir,
        "working",
        &[
            ".weaver-chunks",
            weaver_graph::names::CACHE_DIR,
            ".weaver-staging",
            WORKING_DIR_MARKER,
        ],
        left_behind,
        settings,
        &mut validation,
    )?;
    source_exists
        .then_some(validation)
        .ok_or_else(|| format!("no delivery source exists at {}", working_dir.display()))
}

fn scan_delivery_root(
    root: &Path,
    source_name: &str,
    ignored_entries: &[&str],
    ignored_paths: &HashSet<PathBuf>,
    settings: &PostProcessingSettings,
    validation: &mut DeliverySourceValidation,
) -> Result<bool, String> {
    let root_metadata = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(format!(
                "could not inspect delivery root {}: {error}",
                root.display()
            ));
        }
    };
    if metadata_is_link_or_reparse(&root_metadata) || !root_metadata.is_dir() {
        return Err(format!("unsafe delivery root {}", root.display()));
    }
    let entries = std::fs::read_dir(root).map_err(|error| {
        format!(
            "could not inspect delivery root {}: {error}",
            root.display()
        )
    })?;
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
        let entry_path = entry.path();
        if ignored_paths.contains(&entry_path) {
            continue;
        }
        let entry_name = entry.file_name();
        let mut entry_bytes = 0u64;
        let mut stack = vec![(entry_path.clone(), PathBuf::from(&entry_name))];
        while let Some((path, relative_path)) = stack.pop() {
            let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
                format!(
                    "could not inspect delivery entry {}: {error}",
                    path.display()
                )
            })?;
            if metadata_is_link_or_reparse(&metadata) {
                return Err(format!(
                    "unsafe delivery link or reparse point at {}/{}",
                    source_name,
                    relative_path.display()
                ));
            }
            if metadata.is_dir() {
                let children = std::fs::read_dir(&path).map_err(|error| {
                    format!(
                        "could not inspect delivery directory {}: {error}",
                        path.display()
                    )
                })?;
                for child in children {
                    let child = child.map_err(|error| {
                        format!(
                            "could not inspect delivery directory {}: {error}",
                            path.display()
                        )
                    })?;
                    stack.push((child.path(), relative_path.join(child.file_name())));
                }
                continue;
            }
            if !metadata.is_file() {
                return Err(format!(
                    "unsupported delivery entry at {}/{}",
                    source_name,
                    relative_path.display()
                ));
            }
            let filename = path.file_name().unwrap_or_default().to_string_lossy();
            if settings.unacceptable_extension_match(&filename).is_some() {
                let rejection = UnacceptableExtensionMatch {
                    relative_path: relative_path.to_string_lossy().replace('\\', "/"),
                };
                return Err(crate::post_processing::model::unwanted_extension_reason(
                    &rejection.relative_path,
                ));
            }
            entry_bytes = entry_bytes.saturating_add(metadata.len());
            validation.total_bytes = validation.total_bytes.saturating_add(metadata.len());
        }
        validation.root_entry_bytes.insert(entry_path, entry_bytes);
    }
    Ok(true)
}

// Rejects symlinks and, on Windows, every other reparse point: junctions and
// volume mount points redirect a traversal just as effectively as a symlink.
// `FileType` cannot answer that on stable Rust — `is_reparse_point` is not in
// std, and `FileTypeExt` only exposes the symlink-shaped reparse points — so
// the check reads the raw attribute bit off the metadata each caller already
// holds.
fn metadata_is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    false
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
    file_cache::copy_large_file_with_progress(src, dst, |copied| {
        phase_counters
            .completed_bytes
            .fetch_add(copied, Ordering::Relaxed);
    })?;
    if !runtime_fs::destination_parent_matches(dst, &parent_fingerprint)? {
        return Err(std::io::Error::other(format!(
            "destination parent changed during copy: {}",
            dst.display()
        )));
    }
    std::fs::remove_file(src)?;
    Ok(())
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
    source_bytes: u64,
    phase_counters: Arc<PhaseCounters>,
) -> Result<(), (std::io::Error, std::io::Error)> {
    move_path_with_safe_rename_or_copy_fallback_using(
        src,
        dst,
        source_bytes,
        phase_counters,
        rename_path_for_publication,
        move_path_with_copy_fallback,
    )
}

fn move_path_with_safe_rename_or_copy_fallback_using<R, C>(
    src: &std::path::Path,
    dst: &std::path::Path,
    source_bytes: u64,
    phase_counters: Arc<PhaseCounters>,
    rename: R,
    copy: C,
) -> Result<(), (std::io::Error, std::io::Error)>
where
    R: FnOnce(&std::path::Path, &std::path::Path) -> std::io::Result<()>,
    C: FnOnce(&std::path::Path, &std::path::Path, &PhaseCounters) -> std::io::Result<()>,
{
    match rename(src, dst) {
        Ok(()) => {
            if source_bytes > 0 {
                phase_counters
                    .completed_bytes
                    .fetch_add(source_bytes, Ordering::Relaxed);
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

// Moves one entry of the delivered set into the destination. On a resumed
// move the working directory is authoritative: an entry still present there
// had not finished moving, so whatever already stands at its destination is
// an interrupted copy and gives way to it. A destination entry whose source
// is gone moved whole before the restart, and nothing here reaches it.
// A failed job keeps what it delivered in its working directory, as it
// does on one device: what its staging directory holds goes back there,
// joining the directories both hold, and the staging directory goes. The
// marker and the chunk directory are the staging directory's own.
// The working directory is released whether or not the delivery made it
// back whole: what stays in the staging directory is still the job's, and
// stays there while the job has a record.
pub(crate) fn release_failed_delivery(
    root: &Path,
    working_dir: &Path,
    staging: Option<&Path>,
    job_id: JobId,
) -> std::io::Result<()> {
    release_failed_delivery_using(
        root,
        working_dir,
        staging,
        job_id,
        move_path_with_safe_rename_or_copy_fallback,
    )
}

fn release_failed_delivery_using<M>(
    root: &Path,
    working_dir: &Path,
    staging: Option<&Path>,
    job_id: JobId,
    move_entry: M,
) -> std::io::Result<()>
where
    M: Fn(&Path, &Path, u64, Arc<PhaseCounters>) -> Result<(), (std::io::Error, std::io::Error)>,
{
    let returned = staging.map_or(Ok(()), |staging| {
        return_staged_delivery_using(staging, working_dir, move_entry)
    });
    let released = crate::jobs::working_dir::release_failed_working_dir(root, working_dir, job_id);
    returned.and(released)
}

fn return_staged_delivery_using<M>(
    staging: &Path,
    working_dir: &Path,
    move_entry: M,
) -> std::io::Result<()>
where
    M: Fn(&Path, &Path, u64, Arc<PhaseCounters>) -> Result<(), (std::io::Error, std::io::Error)>,
{
    let entries = match std::fs::read_dir(staging) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        result => result?,
    };
    let counters = Arc::new(PhaseCounters::default());
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        if name == WORKING_DIR_MARKER || name == ".weaver-chunks" {
            continue;
        }
        let (from, to) = (entry.path(), working_dir.join(&name));
        let moved = if std::fs::symlink_metadata(&to).is_ok() {
            merge_delivered_dir(&from, &to, Arc::clone(&counters)).map(|_| ())
        } else {
            let bytes = entry.metadata().map_or(0, |metadata| metadata.len());
            move_entry(&from, &to, bytes, Arc::clone(&counters))
        };
        moved.map_err(|(_, fallback)| fallback)?;
    }
    std::fs::remove_dir_all(staging)
}

// Move what `src` holds into `dst` when both are real directories, entry by
// entry, a directory both hold joined the same way. An entry whose name is
// already taken in `dst` stays where it is. Whether they were directories
// to join.
fn merge_delivered_dir(
    src: &std::path::Path,
    dst: &std::path::Path,
    phase_counters: Arc<PhaseCounters>,
) -> Result<bool, (std::io::Error, std::io::Error)> {
    let real_dir = |path: &std::path::Path| {
        std::fs::symlink_metadata(path)
            .is_ok_and(|metadata| metadata.is_dir() && !metadata_is_link_or_reparse(&metadata))
    };
    if !real_dir(src) || !real_dir(dst) {
        return Ok(false);
    }
    let unread = |error: std::io::Error| {
        (
            error,
            std::io::Error::other("the directory to join was not read"),
        )
    };
    for entry in std::fs::read_dir(src).map_err(unread)? {
        let entry = entry.map_err(unread)?;
        let (from, to) = (entry.path(), dst.join(entry.file_name()));
        if std::fs::symlink_metadata(&to).is_ok() {
            merge_delivered_dir(&from, &to, Arc::clone(&phase_counters))?;
            continue;
        }
        let bytes = std::fs::symlink_metadata(&from)
            .ok()
            .filter(std::fs::Metadata::is_file)
            .map_or(0, |metadata| metadata.len());
        move_path_with_safe_rename_or_copy_fallback(
            &from,
            &to,
            bytes,
            Arc::clone(&phase_counters),
        )?;
    }
    let _ = std::fs::remove_dir(src);
    Ok(true)
}

fn move_delivered_entry(
    src: &std::path::Path,
    dst: &std::path::Path,
    source_bytes: u64,
    phase_counters: Arc<PhaseCounters>,
    resumed: bool,
) -> Result<(), (std::io::Error, std::io::Error)> {
    let interrupted = resumed
        && std::fs::symlink_metadata(dst).is_ok()
        && !runtime_fs::paths_equivalent_for_placement(src, dst);
    if !interrupted {
        return move_path_with_safe_rename_or_copy_fallback(src, dst, source_bytes, phase_counters);
    }
    let not_cleared = |error: std::io::Error| {
        (
            error,
            std::io::Error::other("the interrupted copy at the destination was not cleared"),
        )
    };
    let src_metadata = std::fs::symlink_metadata(src).map_err(not_cleared)?;
    let dst_metadata = std::fs::symlink_metadata(dst).map_err(not_cleared)?;
    let real_dir =
        |metadata: &std::fs::Metadata| metadata.is_dir() && !metadata_is_link_or_reparse(metadata);
    if real_dir(&src_metadata) && real_dir(&dst_metadata) {
        // A directory copied part way: what already left it stands in the
        // destination, and each entry still in it follows the same rule.
        for entry in std::fs::read_dir(src).map_err(not_cleared)? {
            let entry = entry.map_err(not_cleared)?;
            move_delivered_entry(
                &entry.path(),
                &dst.join(entry.file_name()),
                0,
                Arc::clone(&phase_counters),
                true,
            )?;
        }
        return std::fs::remove_dir(src).map_err(not_cleared);
    }
    if real_dir(&dst_metadata) {
        std::fs::remove_dir_all(dst).map_err(not_cleared)?;
    } else {
        std::fs::remove_file(dst).map_err(not_cleared)?;
    }
    move_path_with_safe_rename_or_copy_fallback(src, dst, source_bytes, phase_counters)
}

async fn validate_script_move_destination(
    db: crate::Database,
    context: crate::post_processing::runner::JobExecutionContext,
    expected: &std::path::Path,
) -> Result<(), MoveToCompleteFailure> {
    // A missing directory is an operational failure, not unsafe content.
    if let Err(error) = tokio::fs::symlink_metadata(expected).await
        && error.kind() == std::io::ErrorKind::NotFound
    {
        return Err(MoveToCompleteFailure::Operational(format!(
            "script final directory {} does not exist",
            expected.display()
        )));
    }
    let path = expected.to_path_buf();
    let validated = crate::runtime::panic_trap::spawn_blocking(move || {
        crate::post_processing::effects::validate_directory(&db, &context, &path)
    })
    .await
    .map_err(|error| {
        MoveToCompleteFailure::Operational(format!(
            "script destination validation worker failed: {error}"
        ))
    })?
    .map_err(MoveToCompleteFailure::Security)?;
    if validated != expected {
        return Err(MoveToCompleteFailure::Security(
            "script final directory changed after reservation".into(),
        ));
    }
    Ok(())
}

// Waits for the moving row to commit. A write that failed, or a writer
// lane that closed before running it, fails the move before any file has
// left the working directory, and the still-empty claim is given back.
async fn moving_row_committed(
    durable: tokio::sync::oneshot::Receiver<Result<(), String>>,
    dest: &std::path::Path,
) -> Result<(), MoveToCompleteFailure> {
    let outcome = durable
        .await
        .unwrap_or_else(|_| Err("the database writer closed before it ran".into()));
    if let Err(error) = outcome {
        let _ = tokio::fs::remove_dir(dest).await;
        return Err(MoveToCompleteFailure::Operational(format!(
            "could not record the move before starting it: {error}"
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) type MoveHook = Box<dyn FnOnce(JobId) + Send>;

// Runs once the moving row has committed and before any file moves.
#[cfg(test)]
pub(crate) static BEFORE_FIRST_MOVE: std::sync::Mutex<Option<MoveHook>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) type SpentHook = Box<dyn FnOnce(&[PathBuf]) + Send>;

// Runs with the consumed inputs once their handles are released and before
// they are removed.
#[cfg(test)]
pub(crate) static BEFORE_SPENT_REMOVAL: std::sync::Mutex<Option<SpentHook>> =
    std::sync::Mutex::new(None);

// Where a final move delivers: a directory it claimed empty, or the one
// the moving row of a job interrupted by a restart names, which already
// holds what moved before the restart.
enum MoveInto {
    Claimed(PathBuf),
    Resumed(PathBuf),
}

// What a final move's task is given.
struct FinalMove {
    job_id: JobId,
    working_dir: PathBuf,
    staging_dir: Option<PathBuf>,
    dest: PathBuf,
    // Answers once the moving row has committed. A resumed move has none:
    // its row was already moving when it was read.
    durable: Option<tokio::sync::oneshot::Receiver<Result<(), String>>>,
    script_validation: Option<(
        crate::Database,
        crate::post_processing::runner::JobExecutionContext,
    )>,
    hold: Option<&'static str>,
    phase_counters: Arc<PhaseCounters>,
    naming: Option<DeliveryNamingPlan>,
    policy_source: DeliveryPolicySource,
    // Consumed inputs whose removal failed: they stay where they are.
    left_behind: HashSet<PathBuf>,
}

#[allow(clippy::too_many_arguments)]
async fn run_move_to_complete(
    job_id: JobId,
    working_dir: PathBuf,
    staging_dir: Option<PathBuf>,
    dest: PathBuf,
    phase_counters: Arc<PhaseCounters>,
    naming: Option<DeliveryNamingPlan>,
    policy_source: DeliveryPolicySource,
    left_behind: HashSet<PathBuf>,
) -> Result<MoveToCompleteResult, MoveToCompleteFailure> {
    let claimed_dest = dest.clone();
    let result = run_move_to_claimed_destination(
        job_id,
        working_dir,
        staging_dir,
        MoveInto::Claimed(dest),
        phase_counters,
        naming,
        policy_source,
        left_behind,
    )
    .await;
    if result.is_err() {
        // The actor exclusively created this directory before launching the
        // worker. Remove the abandoned claim only when it is still empty;
        // partial output is deliberately retained for diagnosis and retry.
        let _ = tokio::fs::remove_dir(&claimed_dest).await;
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_move_to_claimed_destination(
    job_id: JobId,
    working_dir: PathBuf,
    staging_dir: Option<PathBuf>,
    dest: MoveInto,
    phase_counters: Arc<PhaseCounters>,
    naming: Option<DeliveryNamingPlan>,
    policy_source: DeliveryPolicySource,
    // Consumed inputs that could not be removed: they stay in the working
    // directory and are neither scanned nor moved.
    left_behind: HashSet<PathBuf>,
) -> Result<MoveToCompleteResult, MoveToCompleteFailure> {
    let (dest, resumed) = match dest {
        MoveInto::Claimed(dest) => (dest, false),
        MoveInto::Resumed(dest) => (dest, true),
    };
    let policy = match policy_source {
        DeliveryPolicySource::Persisted(database) => {
            crate::runtime::panic_trap::spawn_blocking(move || database.post_processing_settings())
                .await
                .map_err(|error| {
                    MoveToCompleteFailure::Operational(format!(
                        "could not load unacceptable extension policy: {error}"
                    ))
                })?
                .map_err(|error| {
                    MoveToCompleteFailure::Operational(format!(
                        "could not load unacceptable extension policy: {error}"
                    ))
                })?
        }
        #[cfg(test)]
        DeliveryPolicySource::Fixed(policy) => policy,
    };
    let policy = Arc::new(policy);
    let validation = {
        let working_dir = working_dir.clone();
        let staging_dir = staging_dir.clone();
        let policy = Arc::clone(&policy);
        let left_behind = left_behind.clone();
        crate::runtime::panic_trap::spawn_blocking(move || {
            validate_delivery_sources(&working_dir, staging_dir.as_deref(), &policy, &left_behind)
        })
        .await
        .map_err(|error| {
            MoveToCompleteFailure::Operational(format!(
                "delivery security scan worker failed: {error}"
            ))
        })?
        .map_err(MoveToCompleteFailure::Security)?
    };
    phase_counters
        .total_bytes
        .store(validation.total_bytes, Ordering::Relaxed);

    let dest_metadata = tokio::fs::symlink_metadata(&dest).await.map_err(|error| {
        MoveToCompleteFailure::Operational(format!(
            "could not inspect claimed complete directory {}: {error}",
            dest.display()
        ))
    })?;
    if !dest_metadata.is_dir() || metadata_is_link_or_reparse(&dest_metadata) {
        return Err(MoveToCompleteFailure::Security(format!(
            "claimed complete destination changed before publication: {}",
            dest.display()
        )));
    }
    let mut existing_entries = tokio::fs::read_dir(&dest).await.map_err(|error| {
        MoveToCompleteFailure::Operational(format!(
            "could not inspect claimed complete directory {}: {error}",
            dest.display()
        ))
    })?;
    if !resumed
        && existing_entries
            .next_entry()
            .await
            .map_err(|error| {
                MoveToCompleteFailure::Operational(format!(
                    "could not inspect claimed complete directory {}: {error}",
                    dest.display()
                ))
            })?
            .is_some()
    {
        return Err(MoveToCompleteFailure::Security(format!(
            "claimed complete directory was populated before publication: {}",
            dest.display()
        )));
    }

    let mut moved = 0u32;
    let mut failures = Vec::new();

    if let Some(ref staging) = staging_dir
        && let Ok(mut entries) = tokio::fs::read_dir(staging).await
    {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name();
            if file_name == WORKING_DIR_MARKER {
                continue;
            }
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
            let source_bytes = validation.root_entry_bytes.get(&src).copied().unwrap_or(0);
            let src_fb = src.clone();
            let dst_fb = dst.clone();
            let counters = Arc::clone(&phase_counters);
            match crate::runtime::panic_trap::spawn_blocking(move || {
                move_delivered_entry(&src_fb, &dst_fb, source_bytes, counters, resumed)
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
                || file_name == weaver_graph::names::CACHE_DIR
                || file_name == ".weaver-staging"
                || file_name == WORKING_DIR_MARKER
            {
                continue;
            }
            let src = entry.path();
            if left_behind.contains(&src) {
                continue;
            }
            let dst = dest.join(&file_name);
            if !resumed && dst.exists() && !runtime_fs::paths_equivalent_for_placement(&src, &dst) {
                // A directory the staging directory delivered first, which
                // the working directory also holds delivered files of: they
                // join it, and a name the staging directory already filled
                // keeps what it holds.
                let counters = Arc::clone(&phase_counters);
                let (src_fb, dst_fb) = (src.clone(), dst.clone());
                match crate::runtime::panic_trap::spawn_blocking(move || {
                    merge_delivered_dir(&src_fb, &dst_fb, counters)
                })
                .await
                {
                    Ok(Ok(true)) => moved += 1,
                    Ok(Ok(false)) => {}
                    Ok(Err((rename_err, copy_err))) => failures.push(format!(
                        "{}: rename failed: {}; fallback failed: {}",
                        file_name.to_string_lossy(),
                        rename_err,
                        copy_err
                    )),
                    Err(join_err) => failures.push(format!(
                        "{}: move task failed: {}",
                        file_name.to_string_lossy(),
                        join_err
                    )),
                }
                continue;
            }
            let source_bytes = validation.root_entry_bytes.get(&src).copied().unwrap_or(0);
            let src_fb = src.clone();
            let dst_fb = dst.clone();
            let counters = Arc::clone(&phase_counters);
            match crate::runtime::panic_trap::spawn_blocking(move || {
                move_delivered_entry(&src_fb, &dst_fb, source_bytes, counters, resumed)
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
        return Err(MoveToCompleteFailure::Operational(format!(
            "failed to move {} entr{} to complete directory: {}",
            failures.len(),
            if failures.len() == 1 { "y" } else { "ies" },
            failures[0]
        )));
    }

    // Both delivery routes have landed in `dest` and nothing else will be added
    // to it, so this is the first and only moment the delivered set exists as
    // one directory. Renaming here is still a same-directory rename, and it
    // finishes before the move reports done — everything downstream of the
    // move sees only the final names.
    let renamed_members = match naming {
        Some(naming) => {
            deobfuscate::rename_obfuscated_members(job_id, &dest, &naming, &policy).await
        }
        None => Vec::new(),
    };

    // The delivered tree is the user's Weaver runs as, with the modes its
    // umask gives, whatever the archives it came out of said.
    let delivered = dest.clone();
    match crate::runtime::panic_trap::spawn_blocking(move || {
        runtime_fs::normalize_extracted_tree(&delivered)
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(MoveToCompleteFailure::Operational(format!(
                "could not set ownership and modes of {}: {error}",
                dest.display()
            )));
        }
        Err(error) => {
            return Err(MoveToCompleteFailure::Operational(format!(
                "ownership and mode worker failed for {}: {error}",
                dest.display()
            )));
        }
    }

    let output_dir = dest.clone();
    match crate::runtime::panic_trap::spawn_blocking(move || {
        mark_weaver_owned_output_dir(&output_dir)
    })
    .await
    {
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

    // The chunk workspace is the job's scratch, and the cache beside it the
    // run's: everything they backed has been published, so they go with the
    // working directory they live in.
    for scratch in [".weaver-chunks", weaver_graph::names::CACHE_DIR] {
        let chunks = working_dir.join(scratch);
        if let Err(error) = tokio::fs::remove_dir_all(&chunks).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                job_id = job_id.0,
                path = %chunks.display(),
                error = %error,
                "failed to remove chunk workspace during final move"
            );
        }
    }

    // The marker goes only with the directory. One still holding something
    // the move left, a consumed input that could not be removed, stays the
    // job's: a history delete, or the next start once the job is gone, takes
    // it whole.
    if holds_more_than_marker(&working_dir).await {
        warn!(
            job_id = job_id.0,
            dir = %working_dir.display(),
            "the intermediate directory keeps what could not be removed; it stays the job's until it can go"
        );
    } else {
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
    }

    Ok(MoveToCompleteResult {
        moved_entries: moved,
        renamed_members,
    })
}

// A directory that cannot be read is taken to hold something: its marker
// stays rather than go on a guess.
async fn holds_more_than_marker(dir: &Path) -> bool {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) => return error.kind() != std::io::ErrorKind::NotFound,
    };
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) if entry.file_name() == WORKING_DIR_MARKER => {}
            Ok(None) => return false,
            Ok(Some(_)) | Err(_) => return true,
        }
    }
}

fn complete_parent_for_category(
    complete_dir: &std::path::Path,
    categories: &[crate::categories::CategoryConfig],
    category: Option<&str>,
) -> Result<PathBuf, String> {
    crate::categories::completion_parent(complete_dir, categories, category)
}

fn complete_destination_candidate(
    parent: &Path,
    dir_name: &str,
    job_id: JobId,
    attempt: u32,
) -> PathBuf {
    match attempt {
        0 => parent.join(dir_name),
        1 => parent.join(weaver_model::files::path_component_with_suffix(
            dir_name,
            &format!(".#{}", job_id.0),
        )),
        _ => parent.join(weaver_model::files::path_component_with_suffix(
            dir_name,
            &format!(".#{}.{}", job_id.0, attempt - 1),
        )),
    }
}

async fn claim_complete_destination_path(
    parent: &Path,
    dir_name: &str,
    job_id: JobId,
    reserved: &HashSet<PathBuf>,
) -> std::io::Result<PathBuf> {
    tokio::fs::create_dir_all(parent).await?;

    let mut attempt = 0u32;
    loop {
        let candidate = complete_destination_candidate(parent, dir_name, job_id, attempt);
        if !reserved.contains(&candidate) {
            match tokio::fs::create_dir(&candidate).await {
                Ok(()) => return Ok(candidate),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        attempt = attempt.checked_add(1).ok_or_else(|| {
            std::io::Error::other("exhausted complete-directory collision suffixes")
        })?;
    }
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

    #[tokio::test]
    async fn script_destination_worker_refuses_foreign_ownership_and_path_changes() {
        use crate::post_processing::model::PipelineOutcome;
        use crate::post_processing::runner::{CompatibilityFacts, JobExecutionContext};

        let temp = tempfile::tempdir().unwrap();
        let complete = temp.path().canonicalize().unwrap();
        let source = complete.join("source");
        let destination = complete.join("destination");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(source.join("payload.bin"), b"payload").unwrap();
        let db = crate::Database::open_in_memory().unwrap();
        let context = JobExecutionContext {
            job_id: 1,
            name: "destination-check".into(),
            nzb_filename: "destination-check.nzb".into(),
            category: None,
            group: None,
            source_url: None,
            working_directory: source.clone(),
            final_directory: destination.clone(),
            pipeline_outcome: PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: CompatibilityFacts {
                complete_dir: Some(complete.clone()),
                ..Default::default()
            },
        };
        validate_script_move_destination(db.clone(), context.clone(), &destination)
            .await
            .unwrap();
        let missing = complete.join("missing");
        let error = validate_script_move_destination(db.clone(), context.clone(), &missing)
            .await
            .unwrap_err();
        assert!(
            matches!(error, MoveToCompleteFailure::Operational(message) if message.contains("does not exist"))
        );
        crate::jobs::working_dir::mark_weaver_owned_working_dir(&complete, &destination, JobId(2))
            .unwrap();
        std::fs::write(destination.join("foreign.bin"), b"foreign").unwrap();
        let error = validate_script_move_destination(db.clone(), context.clone(), &destination)
            .await
            .unwrap_err();
        assert!(
            matches!(error, MoveToCompleteFailure::Security(message) if message.contains("foreign ownership marker"))
        );
        assert_eq!(
            std::fs::read(source.join("payload.bin")).unwrap(),
            b"payload"
        );
        assert_eq!(
            std::fs::read(destination.join("foreign.bin")).unwrap(),
            b"foreign"
        );
        assert!(!destination.join("payload.bin").exists());

        #[cfg(unix)]
        {
            let actual = complete.join("actual");
            let alias = complete.join("alias");
            std::fs::create_dir(&actual).unwrap();
            std::os::unix::fs::symlink(&actual, &alias).unwrap();
            let error = validate_script_move_destination(db, context, &alias)
                .await
                .unwrap_err();
            assert!(
                matches!(error, MoveToCompleteFailure::Security(message) if message.contains("changed after reservation"))
            );
            assert!(actual.exists());
            assert!(
                std::fs::symlink_metadata(alias)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
        }
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
    async fn claim_complete_destination(
        &self,
        job_id: JobId,
        dir_name: &str,
        category: Option<&str>,
    ) -> Result<PathBuf, String> {
        let parent = {
            let cfg = self.config.read().await;
            complete_parent_for_category(&self.complete_dir, &cfg.categories, category)?
        };
        let reserved: HashSet<PathBuf> = self
            .reserved_complete_destinations
            .iter()
            .filter(|(reserved_job_id, _)| **reserved_job_id != job_id)
            .map(|(_, path)| path.clone())
            .collect();
        claim_complete_destination_path(&parent, dir_name, job_id, &reserved)
            .await
            .map_err(|error| {
                format!(
                    "failed to reserve a complete directory under {}: {error}",
                    parent.display()
                )
            })
    }

    // Resolves everything the delivery rename pass needs, on this task, before
    // the move worker is spawned.
    //
    // `None` disables the pass. The worker gets an owned plan rather than a
    // handle to pipeline state so the pass cannot reach back into the
    // orchestrator, and so the outbound lookup — the one step that can take
    // seconds — never runs on the orchestrator's own task.
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
            crc32_by_member_path: member_crc32_by_path(
                &self.graph_delivery(job_id).unwrap_or_default().files,
            ),
            proxies: self.shared_state.proxy_runtime(),
        });

        Some(DeliveryNamingPlan {
            job_display_name: job_name.to_string(),
            srrdb,
        })
    }

    // Move extracted/completed files from the intermediate working directory
    // to the complete directory, organized by category.
    //
    // Layout: `{complete_dir}/[{category}/]{release title}/`, the title as
    // posted rather than the display name (see
    // [`crate::ingest::completed_folder_name`]).
    // On collision, appends `.#<job_id>` (and a numeric suffix if needed).
    //
    // Uses rename() for same-filesystem moves, falls back to copy+delete for cross-FS.
    pub(crate) async fn start_move_to_complete(&mut self, job_id: JobId) -> Result<(), String> {
        self.start_move_to_complete_held(job_id, None).await
    }

    // [`Self::start_move_to_complete`], the move first holding at the named
    // e2e delay hook. The hold is on the move's own task, so the job stands
    // at `Moving` and nothing else waits on it.
    pub(crate) async fn start_move_to_complete_held(
        &mut self,
        job_id: JobId,
        hold: Option<&'static str>,
    ) -> Result<(), String> {
        if self.inflight_moves.contains(&job_id) {
            return Ok(());
        }
        if self.shared_state.is_post_processing_paused() {
            if self.jobs.contains_key(&job_id) {
                self.deferred_moves.insert(job_id);
            }
            return Ok(());
        }
        self.deferred_moves.remove(&job_id);

        // The last gate at which every settlement fact is still in hand, and
        // the last at which refusing costs nothing: nothing has moved yet. The
        // census rebuilds the terminal record from what claimed each payload
        // file, and refuses the delivery outright when a file was never
        // delivered and nothing accounts for it.
        self.reconcile_terminal_delivery(job_id)?;
        // Serialize the final-move gate with pause and cancellation acceptance.
        // An intent recorded before this boundary must hold the move.
        self.report_job(job_id);
        let (move_allowed, paused) = self.shared_state.queue.write(|inner| {
            let Some(row) = inner.job_mut(job_id) else {
                return crate::pipeline::queue::Change::unchanged((false, false));
            };
            if row.intents.cancelled.is_some() {
                return crate::pipeline::queue::Change::unchanged((false, false));
            }
            if row.intents.paused == Some(true) {
                return crate::pipeline::queue::Change::unchanged((false, true));
            }
            row.report.moving = true;
            crate::pipeline::queue::Change::changed((true, false))
        });
        if !move_allowed {
            if paused {
                self.deferred_moves.insert(job_id);
            }
            return Ok(());
        }
        // Updates accepted while the preceding stage awaited I/O belong to
        // this delivery. Read them after claiming the move boundary and before
        // copying its category, name and naming metadata.
        self.apply_pending_job_update(job_id);

        // The storage adapter relinquishes every handle before completion
        // removes consumed inputs or publishes outputs. Unlinking an open
        // file creates a visible .nfs entry on NFS and can fail on Windows.
        // Paused jobs never reach this ownership boundary.
        let spent = self
            .graph_jobs
            .get_mut(&job_id)
            .map(|graph| graph.release_delivery_files())
            .transpose()
            .map_err(str::to_owned)?
            .unwrap_or_default();
        #[cfg(test)]
        {
            let hook = BEFORE_SPENT_REMOVAL.lock().unwrap().take();
            if let Some(hook) = hook {
                hook(&spent);
            }
        }
        // A consumed input another process still holds open, or a share that
        // refuses the unlink, is not part of the delivery and costs it
        // nothing: it stays where it is, and the move leaves it there. No
        // retry: the handles Weaver held are already closed, and an outside
        // holder keeps its own for as long as it likes.
        let mut left_behind = HashSet::new();
        for path in spent {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        path = %path.display(),
                        error = %error,
                        "could not remove a consumed input before delivery; it stays in the working directory"
                    );
                    left_behind.insert(path);
                }
            }
        }

        let (working_dir, staging_dir, job_name, folder_name, category) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Err(format!("job {} not found for final move", job_id.0));
            };
            (
                state.working_dir.clone(),
                state.staging_dir.clone(),
                state.spec.name.clone(),
                crate::ingest::completed_folder_name(&state.spec.name, &state.spec.metadata),
                state.spec.category.clone(),
            )
        };

        let requested = self
            .db
            .job_script_effects(job_id.0)
            .map_err(|error| error.to_string())?
            .final_directory;
        let (dest, script_validation) = if let Some(path) = requested {
            let event = self
                .queue_script_context(
                    job_id,
                    crate::post_processing::model::QueueEvent::NzbDownloaded,
                )
                .ok_or("script job disappeared before final move")?;
            let context = crate::post_processing::runner::JobExecutionContext {
                job_id: job_id.0,
                name: job_name.clone(),
                nzb_filename: format!("{job_name}.nzb"),
                category: category.clone(),
                group: None,
                source_url: None,
                working_directory: working_dir.clone(),
                final_directory: path.clone(),
                pipeline_outcome: crate::post_processing::model::PipelineOutcome::Succeeded,
                par_status: 0,
                unpack_status: 0,
                compatibility: event.facts,
            };
            if self
                .reserved_complete_destinations
                .iter()
                .any(|(owner, reserved)| *owner != job_id && reserved == &path)
            {
                return Err("script final directory is reserved by another job".into());
            }
            (path, Some((self.db.clone(), context)))
        } else {
            (
                self.claim_complete_destination(job_id, &folder_name, category.as_deref())
                    .await?,
                None,
            )
        };
        self.phase_end(job_id, JobPhase::Extracting);
        self.phase_end(job_id, JobPhase::Repairing);
        let phase_counters = self.phase_begin(job_id, JobPhase::Moving, None);
        self.reserved_complete_destinations
            .insert(job_id, dest.clone());
        // The moving row keeps what the move leaves, relative to the working
        // directory, so a restart that finds the job elsewhere leaves the
        // same inputs.
        if let Some(state) = self.jobs.get_mut(&job_id) {
            let mut kept: Vec<PathBuf> = left_behind
                .iter()
                .map(|path| {
                    path.strip_prefix(&working_dir)
                        .map_or_else(|_| path.clone(), Path::to_path_buf)
                })
                .collect();
            kept.sort();
            state.delivery_left_behind = kept;
        }
        // The moving row, naming `dest`, must be durable before the first file
        // leaves the working directory: a restart then finds the delivery and
        // never fetches the job again. SABnzbd keeps a finished download in
        // its saved postprocessor queue (sabnzbd/postproc.py:144) and marks it
        // moving before the first file moves (sabnzbd/postproc.py:517);
        // NZBGet writes the post stage into its queue file
        // (daemon/queue/DiskState.cpp:552).
        let durable = self
            .transition_postprocessing_status(job_id, JobStatus::Moving, Some("moving"))
            .unwrap_or_else(|| {
                let (failed, durable) = tokio::sync::oneshot::channel();
                let _ = failed.send(Err("the write could not be queued".into()));
                durable
            });
        self.inflight_moves.insert(job_id);

        self.emit_job_event(PipelineEvent::MoveToCompleteStarted { job_id });
        self.report_job(job_id);

        let naming = self.delivery_naming_plan(job_id, &job_name).await;
        self.spawn_final_move(FinalMove {
            job_id,
            working_dir,
            staging_dir,
            dest,
            durable: Some(durable),
            script_validation,
            hold,
            phase_counters,
            naming,
            policy_source: DeliveryPolicySource::Persisted(self.db.clone()),
            left_behind,
        });
        Ok(())
    }

    // A job restored from a moving row goes on with its move: what is still
    // in its working directory moves into the destination the row names. A
    // working directory that is gone with the destination in place means
    // the move finished, and the job goes on to its terminal pass.
    pub(crate) async fn resume_restored_move(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let recorded = state.working_dir.clone();
        let staging_dir = state.staging_dir.clone();
        let job_name = state.spec.name.clone();
        let folder_name =
            crate::ingest::completed_folder_name(&state.spec.name, &state.spec.metadata);
        let category = state.spec.category.clone();
        let root = self.intermediate_dir.clone();
        let named = recorded.clone();
        let found = tokio::task::spawn_blocking(move || {
            let source = crate::jobs::working_dir::find_job_working_dir(&root, job_id)?;
            let own = source.as_ref() == Some(&named)
                || crate::jobs::working_dir::is_job_working_dir(&root, &named, job_id)?;
            Ok::<_, std::io::Error>((source, own))
        })
        .await
        .map_err(std::io::Error::other)
        .and_then(|found| found);
        let (source, names_working_dir) = match found {
            Ok(found) => found,
            Err(error) => {
                self.fail_job(
                    job_id,
                    format!("could not find what is left to move after a restart: {error}"),
                );
                return;
            }
        };
        // A row that names the job's own working directory, not a
        // destination, was written before the destination was recorded; the
        // move claims one. A destination is told from it by the job's
        // ownership marker, never by where it sits: a complete directory may
        // lie inside the intermediate one, and its destination is resumed
        // into, not claimed again beside itself.
        let dest = if names_working_dir {
            match self
                .claim_complete_destination(job_id, &folder_name, category.as_deref())
                .await
            {
                Ok(dest) => dest,
                Err(error) => {
                    self.fail_job(job_id, error);
                    return;
                }
            }
        } else {
            recorded
        };
        let delivered = tokio::fs::symlink_metadata(&dest)
            .await
            .is_ok_and(|metadata| metadata.is_dir());
        let Some(working_dir) = source else {
            if delivered {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.working_dir = dest;
                    state.staging_dir = None;
                }
                info!(
                    job_id = job_id.0,
                    "final move had finished before the restart"
                );
                self.start_terminal_post_processing(job_id);
            } else {
                self.fail_job(
                    job_id,
                    format!(
                        "neither the working directory nor the destination {} survived the restart",
                        dest.display()
                    ),
                );
            }
            return;
        };
        if !delivered {
            self.fail_job(
                job_id,
                format!(
                    "the destination {} of an interrupted move is gone",
                    dest.display()
                ),
            );
            return;
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.working_dir = working_dir.clone();
        }
        self.shared_state.queue.write(|inner| {
            let Some(row) = inner.job_mut(job_id) else {
                return crate::pipeline::queue::Change::unchanged(());
            };
            row.report.moving = true;
            crate::pipeline::queue::Change::changed(())
        });
        let phase_counters = self.phase_begin(job_id, JobPhase::Moving, None);
        self.reserved_complete_destinations
            .insert(job_id, dest.clone());
        self.inflight_moves.insert(job_id);
        self.emit_job_event(PipelineEvent::MoveToCompleteStarted { job_id });
        self.report_job(job_id);
        info!(
            job_id = job_id.0,
            dest = %dest.display(),
            "resuming final move after a restart"
        );
        let naming = self.delivery_naming_plan(job_id, &job_name).await;
        // The consumed inputs the move was told to leave before the restart
        // stay where they are.
        let left_behind = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .delivery_left_behind
                    .iter()
                    .map(|path| working_dir.join(path))
                    .collect()
            })
            .unwrap_or_default();
        self.spawn_final_move(FinalMove {
            job_id,
            working_dir,
            staging_dir,
            dest,
            durable: None,
            script_validation: None,
            hold: None,
            phase_counters,
            naming,
            policy_source: DeliveryPolicySource::Persisted(self.db.clone()),
            left_behind,
        });
    }

    fn spawn_final_move(&self, final_move: FinalMove) {
        let FinalMove {
            job_id,
            working_dir,
            staging_dir,
            dest,
            durable,
            script_validation,
            hold,
            phase_counters,
            naming,
            policy_source,
            left_behind,
        } = final_move;
        let move_done_tx = self.move_done_tx.clone();
        debug!(
            job_id = job_id.0,
            dest = %dest.display(),
            resumed = durable.is_none(),
            "starting final move"
        );
        // The move is the job's work, and so is every blocking thread it
        // hands work to.
        tokio::spawn(crate::runtime::panic_trap::in_job(job_id, async move {
            if let Some(hook) = hold {
                // The hook sleeps its thread, so it is given one of its own.
                let _ = crate::runtime::panic_trap::spawn_blocking(move || {
                    crate::e2e_failpoint::maybe_delay(hook);
                })
                .await;
            }
            let move_started = Instant::now();
            let result = async {
                let into = match durable {
                    Some(durable) => {
                        moving_row_committed(durable, &dest).await?;
                        MoveInto::Claimed(dest.clone())
                    }
                    None => MoveInto::Resumed(dest.clone()),
                };
                #[cfg(test)]
                {
                    let hook = BEFORE_FIRST_MOVE.lock().unwrap().take();
                    if let Some(hook) = hook {
                        let _ = tokio::task::spawn_blocking(move || hook(job_id)).await;
                    }
                }
                if let Some((db, context)) = script_validation {
                    validate_script_move_destination(db, context, &dest).await?;
                }
                match into {
                    MoveInto::Claimed(dest) => {
                        run_move_to_complete(
                            job_id,
                            working_dir,
                            staging_dir,
                            dest,
                            phase_counters,
                            naming,
                            policy_source,
                            left_behind,
                        )
                        .await
                    }
                    resumed => {
                        run_move_to_claimed_destination(
                            job_id,
                            working_dir,
                            staging_dir,
                            resumed,
                            phase_counters,
                            naming,
                            policy_source,
                            left_behind,
                        )
                        .await
                    }
                }
            }
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
        }));
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
                if !outcome.renamed_members.is_empty() {
                    self.metrics
                        .deobfuscated_members_renamed
                        .fetch_add(outcome.renamed_members.len() as u64, Ordering::Relaxed);
                }
                self.record_file_renames(job_id, &outcome.renamed_members);
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

                self.emit_job_event(PipelineEvent::MoveToCompleteFinished { job_id });
                debug!(
                    job_id = job_id.0,
                    moved = outcome.moved_entries,
                    dest = %dest.display(),
                    "built-in pipeline completed final move"
                );
                self.start_terminal_post_processing(job_id);
            }
            Err(MoveToCompleteFailure::Security(error)) => {
                self.fail_delivery_security_check(job_id, error);
            }
            Err(MoveToCompleteFailure::Operational(error)) => self.fail_job(job_id, error),
        }
    }

    fn start_terminal_post_processing(&mut self, job_id: JobId) {
        self.start_terminal_post_processing_with_outcome(
            job_id,
            crate::post_processing::model::PipelineOutcome::Succeeded,
            None,
        );
    }

    // Resolve the job's script list and, when it is non-empty, run it.
    //
    // Resolution happens here rather than at submission time: the list a job
    // runs is the one configured when it finishes, which is what both oracles
    // do and what removes the "edited while queued" race entirely.
    pub(crate) fn start_terminal_post_processing_with_outcome(
        &mut self,
        job_id: JobId,
        pipeline_outcome: crate::post_processing::model::PipelineOutcome,
        primary_failure: Option<String>,
    ) {
        let facts = self.fresh_post_processing_facts(job_id, pipeline_outcome);
        self.start_terminal_post_processing_with_facts(job_id, facts, primary_failure);
    }

    // Resolve and run the job's script list, telling the scripts `facts`
    // about how the download ended.
    //
    // The script settings are read off the actor and the answer comes back
    // as an event: the job is held by its terminal pass from here, so the
    // queue window, slot admission and a rename all leave it alone meanwhile.
    fn start_terminal_post_processing_with_facts(
        &mut self,
        job_id: JobId,
        facts: crate::post_processing::model::PostProcessingFacts,
        primary_failure: Option<String>,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        if !self.inflight_terminal_post_processing.insert(job_id) {
            return;
        }
        let category = state.spec.category.clone();
        let executor = self.terminal_post_processing_executor.clone();
        let db = self.db.clone();
        let done_tx = self.terminal_post_processing_done_tx.clone();
        tokio::spawn(async move {
            let read = tokio::task::spawn_blocking(move || {
                let admission = executor.admit_job_scripts(category.as_deref());
                let queue_events_may_hold = match &admission {
                    Ok(Some(admission)) => {
                        !admission.has_enabled_entries() && queue_events_may_hold_job(&db, job_id)
                    }
                    _ => false,
                };
                (admission, queue_events_may_hold)
            })
            .await;
            let (admission, queue_events_may_hold) = read.unwrap_or_else(|error| {
                (
                    Err(crate::StateError::Database(format!(
                        "the script settings read did not finish: {error}"
                    ))),
                    false,
                )
            });
            let _ = done_tx
                .send(TerminalPostProcessingEvent::ScriptsAdmitted(
                    TerminalScriptsAdmitted {
                        job_id,
                        facts,
                        primary_failure,
                        admission,
                        queue_events_may_hold,
                    },
                ))
                .await;
        });
    }

    // Run the scripts the settings admitted, or finish the job when none
    // can be involved.
    pub(in crate::pipeline) fn handle_terminal_scripts_admitted(
        &mut self,
        admitted: TerminalScriptsAdmitted,
    ) {
        let TerminalScriptsAdmitted {
            job_id,
            facts,
            primary_failure,
            admission,
            queue_events_may_hold,
        } = admitted;
        self.inflight_terminal_post_processing.remove(&job_id);
        // A cancellation archived the job while its settings were read.
        if !self.jobs.contains_key(&job_id) {
            return;
        }
        let admission = match admission {
            Ok(Some(admission)) => admission,
            Ok(None) => {
                self.finish_without_terminal_scripts(job_id, primary_failure);
                return;
            }
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "could not admit post-processing scripts; skipping scripts"
                );
                self.finish_without_terminal_scripts(job_id, primary_failure);
                return;
            }
        };
        // With no post-processing instance the job may still have queue
        // events of its own queued or running; the terminal pass waits for
        // those before the job leaves the queue, so it is skipped only when
        // no queue script can be involved.
        if !admission.has_enabled_entries() && !queue_events_may_hold {
            self.finish_without_terminal_scripts(job_id, primary_failure);
            return;
        }
        self.inflight_terminal_post_processing.insert(job_id);
        // Low-frequency: at most one terminal post-processing pass per job.
        self.note_stage_started(
            job_id,
            crate::operations::instrumentation::JobStageKind::PostProcess,
        );
        self.launch_terminal_post_processing_run(job_id, admission, facts, primary_failure, None);
    }

    fn finish_without_terminal_scripts(&mut self, job_id: JobId, primary_failure: Option<String>) {
        match primary_failure {
            Some(failure) => {
                self.finalize_failed_job_after_terminal_post_processing(job_id, failure);
            }
            None => self.complete_job_after_terminal_post_processing(job_id),
        }
    }

    // What the scripts of a pass beginning now are told about how the job's
    // download ended. The PAR and unpack statuses come from what this run of
    // weaver saw, which a restart loses, so they are kept with the outcome.
    pub(crate) fn fresh_post_processing_facts(
        &self,
        job_id: JobId,
        outcome: crate::post_processing::model::PipelineOutcome,
    ) -> crate::post_processing::model::PostProcessingFacts {
        let pipeline_failure_stage = match &outcome {
            crate::post_processing::model::PipelineOutcome::Failed { stage, .. } => Some(*stage),
            crate::post_processing::model::PipelineOutcome::Succeeded => None,
        };
        let verdict = self.delivered_verdict(job_id);
        let par_status = if matches!(
            pipeline_failure_stage,
            Some(
                crate::post_processing::model::PipelineFailureStage::Verify
                    | crate::post_processing::model::PipelineFailureStage::Repair
            )
        ) {
            1
        } else if verdict
            .as_ref()
            .is_some_and(|verdict| verdict.recovery_verified)
        {
            2
        } else {
            0
        };
        let unpack_status = if matches!(
            pipeline_failure_stage,
            Some(crate::post_processing::model::PipelineFailureStage::Extract)
        ) {
            1
        } else if verdict.as_ref().is_some_and(|verdict| verdict.unpacked) {
            2
        } else {
            0
        };
        let delivered = verdict.filter(|_| pipeline_failure_stage.is_none());
        let left_behind = self
            .jobs
            .get(&job_id)
            .map(|state| state.delivery_left_behind.clone())
            .unwrap_or_default();
        crate::post_processing::model::PostProcessingFacts {
            outcome,
            par_status,
            unpack_status,
            delivered,
            left_behind,
        }
    }

    // What the job's delivery came to: its graph's ruling while it has one,
    // else the ruling its row kept when its final move began.
    pub(in crate::pipeline) fn delivered_verdict(
        &self,
        job_id: JobId,
    ) -> Option<crate::post_processing::model::DeliveredVerdict> {
        self.graph_delivered_verdict(job_id)
            .or_else(|| self.jobs.get(&job_id)?.delivered.clone())
    }

    // The context a job's scripts are handed, told `facts` about how its
    // download ended.
    pub(crate) fn terminal_post_processing_context(
        &self,
        job_id: JobId,
        facts: &crate::post_processing::model::PostProcessingFacts,
    ) -> Option<crate::post_processing::runner::JobExecutionContext> {
        // Scripts read health to decide whether the download is worth acting
        // on, so they must be handed the settled figure — the one the terminal
        // record will carry — and not the live wire counter the settlement has
        // already answered.
        let settled_health = self
            .jobs
            .get(&job_id)
            .map(|state| state.spec.total_bytes)
            .map(|total_bytes| self.terminal_record_figures(job_id, total_bytes).1);
        let state = self.jobs.get(&job_id)?;
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
        let failure_message = match &facts.outcome {
            crate::post_processing::model::PipelineOutcome::Failed { message, .. } => {
                Some(message.clone())
            }
            crate::post_processing::model::PipelineOutcome::Succeeded => None,
        };
        Some(crate::post_processing::runner::JobExecutionContext {
            job_id: job_id.0,
            name: state.spec.name.clone(),
            nzb_filename: format!("{}.nzb", state.spec.name),
            category: state.spec.category.clone(),
            group: None,
            source_url: crate::post_processing::scan::source_url(&state.spec.metadata),
            working_directory: state.working_dir.clone(),
            final_directory: state.working_dir.clone(),
            pipeline_outcome: facts.outcome.clone(),
            par_status: facts.par_status,
            unpack_status: facts.unpack_status,
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
                parameters: state.spec.metadata.clone(),
                marked_bad: false,
                final_directory_override: None,
            },
        })
    }

    fn launch_terminal_post_processing_run(
        &mut self,
        job_id: JobId,
        admission: crate::post_processing::executor::PostProcessingJobAdmission,
        facts: crate::post_processing::model::PostProcessingFacts,
        primary_failure: Option<String>,
        resume: Option<crate::post_processing::model::PostProcessingResume>,
    ) {
        let Some(context) = self.terminal_post_processing_context(job_id, &facts) else {
            self.inflight_terminal_post_processing.remove(&job_id);
            return;
        };
        // Kept on the job before anything records it as in post-processing,
        // in the same ordered write lane, so a restart resumes the scripts
        // telling them what the ones before it were told.
        if let Err(error) =
            self.db
                .try_queue_job_write(job_id, "record_job_post_processing_facts", move |db| {
                    db.record_job_post_processing_facts(job_id.0, &facts)
                })
        {
            warn!(
                job_id = job_id.0,
                error = %error,
                "could not queue the post-processing outcome write"
            );
        }
        let delivered_dir = primary_failure
            .is_none()
            .then(|| context.working_directory.clone());
        self.transition_postprocessing_status(
            job_id,
            JobStatus::QueuedPostProcessing,
            Some("queued for post-processing scripts"),
        );
        // A delivered job's output now lives only at its final location, and
        // the scripts may wait on queue events before the executor records
        // that they started. Make that fact durable first, in the job's
        // ordered write lane, so a restart in that window resumes the scripts
        // against the delivered output instead of re-finalizing a working
        // directory the move already emptied. A failed job is recorded too:
        // the outcome written above tells its restored scripts it failed.
        self.persist_active_runtime_at(job_id, delivered_dir);
        self.report_job(job_id);
        let (cancellation_tx, cancellation_rx) = tokio::sync::watch::channel(false);
        self.terminal_post_processing_cancellations
            .insert(job_id, cancellation_tx);
        let executor = self.terminal_post_processing_executor.clone();
        let done_tx = self.terminal_post_processing_done_tx.clone();
        let db = self.db.clone();
        tokio::spawn(async move {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let execution = async {
                let mut cancellation_wait = cancellation_rx.clone();
                if !*cancellation_wait.borrow() {
                    tokio::select! {
                        result = crate::post_processing::events::wait_for_job_events(&db, job_id.0) => {
                            // A queue event whose scripts could not run is recorded
                            // with that run; it does not fail post-processing.
                            if let Err(error) = result {
                                tracing::warn!(job_id = job_id.0, %error, "queue scripts could not run before post-processing");
                            }
                        }
                        _ = cancellation_wait.changed() => {},
                    }
                }
                match resume {
                    Some(resume) => {
                        executor
                            .resume_admitted_job(
                                job_id.0,
                                admission,
                                context,
                                Some(cancellation_rx),
                                Some(started_tx),
                                resume,
                            )
                            .await
                    }
                    None => {
                        executor
                            .execute_admitted_job(
                                job_id.0,
                                admission,
                                context,
                                Some(cancellation_rx),
                                Some(started_tx),
                            )
                            .await
                    }
                }
            };
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

    // Finish a job that was restored while it sat in post-processing.
    //
    // The startup recovery scan already stamped `interrupted` on every such
    // job. No script that had started is rerun: one that was mid-flight when
    // weaver stopped has unknown side effects, and running it again is worse
    // than reporting that it was interrupted. The scripts of the list that
    // had not started still run, after the ones that had.
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
        // What the job's scripts were told when its pass began. A job in
        // post-processing does not carry its download's failure in its
        // status, so a failed one's comes back from here.
        let facts = match self.db.job_post_processing_facts(job_id.0) {
            Ok(facts) => facts,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "could not read how the restored job's download ended"
                );
                None
            }
        };
        // The failing stage survives the restart here too, so the history row
        // still records why the job failed.
        if let Some(crate::post_processing::model::PipelineOutcome::Failed { stage, .. }) =
            facts.as_ref().map(|facts| &facts.outcome)
        {
            self.pipeline_failure_stages.entry(job_id).or_insert(*stage);
        }
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
            match facts {
                Some(facts) => {
                    let primary_failure = facts.primary_failure();
                    self.start_terminal_post_processing_with_facts(job_id, facts, primary_failure);
                }
                None => self.start_terminal_post_processing(job_id),
            }
            return true;
        }
        if self.resume_restored_terminal_post_processing(job_id, summary, facts.clone()) {
            return true;
        }
        let results = self
            .db
            .job_post_processing_results(job_id.0)
            .unwrap_or_default();
        let primary_failure = self
            .jobs
            .get(&job_id)
            .and_then(|state| state.failure_error.clone())
            .or_else(|| facts.as_ref().and_then(|facts| facts.primary_failure()));
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

    // Run the rest of an interrupted pass, or return false to finish the job
    // with what it recorded. Only a pass cut off part way through resumes,
    // and only when the job kept the list of entries that had started: a
    // pass begun under an older weaver did not, and which of its scripts ran
    // is unknown. A failed job resumes like a delivered one, its scripts
    // told the outcome the pass began with; one whose pass began under a
    // weaver that did not keep that outcome is finished as it stands.
    fn resume_restored_terminal_post_processing(
        &mut self,
        job_id: JobId,
        summary: crate::post_processing::model::PostProcessingSummary,
        facts: Option<crate::post_processing::model::PostProcessingFacts>,
    ) -> bool {
        use crate::post_processing::model::PostProcessingSummary;
        if !matches!(
            summary,
            PostProcessingSummary::Interrupted | PostProcessingSummary::Running
        ) {
            return false;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let facts = match facts {
            Some(facts) => facts,
            None if state.failure_error.is_some() => return false,
            None => self.fresh_post_processing_facts(
                job_id,
                crate::post_processing::model::PipelineOutcome::Succeeded,
            ),
        };
        let primary_failure = facts.primary_failure();
        let category = state.spec.category.clone();
        let resume = match self.db.job_post_processing_resume(job_id.0) {
            Ok(Some(resume)) => resume,
            Ok(None) => return false,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "could not read which post-processing scripts had started"
                );
                return false;
            }
        };
        let admission = match self
            .terminal_post_processing_executor
            .admit_job_scripts(category.as_deref())
        {
            Ok(Some(admission)) => admission,
            Ok(None) => return false,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "could not admit post-processing scripts; finishing without them"
                );
                return false;
            }
        };
        if !self.inflight_terminal_post_processing.insert(job_id) {
            return true;
        }
        info!(
            job_id = job_id.0,
            started = resume.started.len(),
            "resuming post-processing with the scripts that had not started"
        );
        self.note_stage_started(
            job_id,
            crate::operations::instrumentation::JobStageKind::PostProcess,
        );
        self.launch_terminal_post_processing_run(
            job_id,
            admission,
            facts,
            primary_failure,
            Some(resume),
        );
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
        self.report_job(job_id);
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
        match self.db.job_script_effects(done.job_id.0) {
            Ok(effects) => {
                if let Some(state) = self.jobs.get_mut(&done.job_id) {
                    effects.merge_parameters(&mut state.spec.metadata);
                    if let Some(directory) = effects.directory {
                        state.working_dir = directory;
                    }
                    if let Some(directory) = effects.final_directory {
                        state.working_dir = directory;
                    }
                }
                if effects.marked_bad {
                    self.finalize_failed_job_after_terminal_post_processing(
                        done.job_id,
                        "FAILURE/BAD: marked bad by script".into(),
                    );
                    return;
                }
            }
            Err(error) => {
                self.finalize_failed_job_after_terminal_post_processing(
                    done.job_id,
                    format!("could not restore applied script directives: {error}"),
                );
                return;
            }
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
            self.emit_job_event(PipelineEvent::DownloadFinished {
                job_id,
                finalization_pending: false,
            });
        }
        self.jobs_finalizing_download.remove(&job_id);
        self.job_order.retain(|id| *id != job_id);
        info!(
            job_id = job_id.0,
            "job completed after terminal post-processing"
        );
        self.record_job_history(job_id, Some(PipelineEvent::JobCompleted { job_id }));
        self.report_job(job_id);
    }
}

// Whether a queue script could have an event of this job queued or running.
// The hint is worked out afresh when a settings or instance save dropped it,
// so an unknown hint never sends a job without queue scripts through the
// terminal pass.
fn queue_events_may_hold_job(db: &crate::Database, job_id: JobId) -> bool {
    if let Err(error) = crate::post_processing::events::refresh_admission_hint(db) {
        warn!(job_id = job_id.0, %error, "could not read the queue script settings");
    }
    db.queue_barrier_possible(job_id.0)
}

#[cfg(test)]
mod tests;
