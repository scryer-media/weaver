use super::checkpoint::{
    DirectOutputWriter, ExtractionCheckpointState, FinalizeMemberContext, SharedOutputFile,
};
use super::readahead::ReadaheadVolumeProvider;
use super::source::BoundedRarSourcePool;
use super::*;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};

pub(crate) struct RarExtractionContext<'a> {
    pub(crate) volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
    pub(crate) event_tx: &'a broadcast::Sender<PipelineEvent>,
    pub(crate) job_id: JobId,
    pub(crate) set_name: &'a str,
    pub(crate) output_dir: &'a std::path::Path,
    pub(crate) root: Option<Arc<ExtractionRoot>>,
    pub(crate) budget: Option<Arc<JobExtractionBudget>>,
    pub(crate) options: &'a unrar_rs::ExtractOptions,
    pub(crate) phase_attempt: Option<Arc<PhaseAttemptCounters>>,
}

struct PhaseAttemptRollbackGuard {
    attempt: Option<Arc<PhaseAttemptCounters>>,
}

impl PhaseAttemptRollbackGuard {
    fn new(attempt: Option<Arc<PhaseAttemptCounters>>) -> Self {
        Self { attempt }
    }

    fn attempt(&self) -> Option<Arc<PhaseAttemptCounters>> {
        self.attempt.as_ref().map(Arc::clone)
    }

    fn reserve_total(&self, bytes: u64) {
        if let Some(attempt) = &self.attempt {
            attempt.reserve_total(bytes);
        }
    }

    fn commit(mut self) {
        if let Some(attempt) = self.attempt.take() {
            attempt.commit();
        }
    }
}

impl Drop for PhaseAttemptRollbackGuard {
    fn drop(&mut self) {
        if let Some(attempt) = &self.attempt {
            attempt.rollback();
        }
    }
}

const RAR_MAX_DICT_ENV: &str = "WEAVER_RAR_MAX_DICT_BYTES";
/// Engine default (256 MiB) refuses every real RAR7 archive, whose
/// dictionaries are >4 GiB by construction. Floor of the scaled default.
const RAR_MAX_DICT_FLOOR_BYTES: u64 = 256 * 1024 * 1024;
/// unrar's own compatibility ceiling for declared dictionary sizes.
const RAR_MAX_DICT_CEILING_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Maximum RAR dictionary size the server will decode.
///
/// `WEAVER_RAR_MAX_DICT_BYTES` overrides; otherwise half of physical memory,
/// clamped to [256 MiB, 64 GiB]. The dictionary window is the dominant
/// allocation when extracting solid/big-dictionary archives, so this scales
/// the admission policy with the machine instead of refusing all RAR7 input.
fn configured_rar_max_dict_bytes() -> u64 {
    static CONFIGURED: OnceLock<u64> = OnceLock::new();
    *CONFIGURED.get_or_init(|| {
        if let Ok(value) = std::env::var(RAR_MAX_DICT_ENV) {
            match value.trim().parse::<u64>() {
                Ok(bytes) if bytes > 0 => return bytes,
                _ => tracing::warn!(
                    env = RAR_MAX_DICT_ENV,
                    value,
                    "invalid RAR max dictionary override; using memory-scaled default"
                ),
            }
        }
        crate::runtime::system_probe::detect_total_memory_bytes()
            .map(|total| (total / 2).clamp(RAR_MAX_DICT_FLOOR_BYTES, RAR_MAX_DICT_CEILING_BYTES))
            .unwrap_or(RAR_MAX_DICT_FLOOR_BYTES)
    })
}

/// Apply the server's decode limits to a freshly opened archive.
pub(crate) fn apply_server_rar_limits(archive: &mut unrar_rs::RarArchive) {
    apply_server_rar_limits_with_memory_limit(archive, u64::MAX);
}

fn apply_server_rar_limits_with_memory_limit(
    archive: &mut unrar_rs::RarArchive,
    extraction_memory_limit: u64,
) {
    let limits = unrar_rs::Limits {
        max_dict_size: configured_rar_max_dict_bytes().min(extraction_memory_limit),
        ..Default::default()
    };
    archive.set_limits(limits);
}

fn rar_decoder_memory_bytes(archive: &unrar_rs::RarArchive) -> u64 {
    archive
        .metadata()
        .members
        .iter()
        .map(|member| member.compression.dict_size)
        .max()
        .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RarArchiveOpenMode {
    AttachOnly,
    RefreshProvidedVolumes,
}

pub(crate) struct RarExtractionOpenRequest<'a> {
    pub(crate) set_name: &'a str,
    pub(crate) volume_paths: std::collections::BTreeMap<u32, PathBuf>,
    pub(crate) password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    pub(crate) cached_headers: Option<Vec<u8>>,
    pub(crate) shared_kdf_cache: std::sync::Arc<unrar_rs::crypto::KdfCache>,
    pub(crate) open_mode: RarArchiveOpenMode,
    pub(crate) requested_members: &'a [String],
    pub(crate) already_extracted: Option<&'a std::collections::HashSet<String>>,
    pub(crate) budget: Option<Arc<JobExtractionBudget>>,
}

pub(crate) struct RarArchiveSnapshotOpenRequest<'a> {
    pub(crate) set_name: &'a str,
    pub(crate) volume_paths: std::collections::BTreeMap<u32, PathBuf>,
    pub(crate) password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    pub(crate) cached_headers: Option<Vec<u8>>,
    pub(crate) shared_kdf_cache: std::sync::Arc<unrar_rs::crypto::KdfCache>,
    pub(crate) open_mode: RarArchiveOpenMode,
    pub(crate) requested_members: Option<&'a [String]>,
    pub(crate) already_extracted: Option<&'a std::collections::HashSet<String>>,
    pub(crate) budget: Option<Arc<JobExtractionBudget>>,
}

struct RarArchiveOpenInputs<'a> {
    set_name: &'a str,
    volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
    cached_headers: Option<&'a [u8]>,
    shared_kdf_cache: std::sync::Arc<unrar_rs::crypto::KdfCache>,
    open_mode: RarArchiveOpenMode,
    requested_members: Option<&'a [String]>,
    already_extracted: Option<&'a std::collections::HashSet<String>>,
    budget: Option<Arc<JobExtractionBudget>>,
}

struct BudgetedRarVolumeProvider<'a, P> {
    inner: &'a P,
    budget: Arc<JobExtractionBudget>,
}

impl<'a, P> BudgetedRarVolumeProvider<'a, P> {
    fn new(inner: &'a P, budget: Arc<JobExtractionBudget>) -> Self {
        Self { inner, budget }
    }
}

impl<P: unrar_rs::VolumeProvider> unrar_rs::VolumeProvider for BudgetedRarVolumeProvider<'_, P> {
    fn get_volume(
        &self,
        index: usize,
    ) -> Result<Box<dyn unrar_rs::ReadSeek>, unrar_rs::VolumeProviderError> {
        let reader = self.inner.get_volume(index)?;
        Ok(Box::new(BudgetedReader::new(
            reader,
            Arc::clone(&self.budget),
        )))
    }
}

pub(crate) struct RarExtractionOpenSelection {
    pub(crate) archive: unrar_rs::RarArchive,
    pub(crate) password: Option<String>,
    pub(crate) validated_password: Option<String>,
    pub(crate) decoder_memory_bytes: u64,
}

/// Rejects any path that would escape the directory it is joined onto: absolute
/// paths, `..`, root and prefix components, Windows drive letters, embedded NUL,
/// and the empty path. Shared with the direct-store coverage snapshot through
/// `pipeline::extraction`'s re-export — one validator, one stance.
pub(crate) fn validate_sanitized_rar_member_path(member_name: &str) -> Result<PathBuf, String> {
    if member_name.contains('\0') {
        return Err(format!("unsafe RAR member path: {member_name}"));
    }

    let path = Path::new(member_name);
    if member_name.is_empty() || path.is_absolute() {
        return Err(format!("unsafe RAR member path: {member_name}"));
    }

    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let value = part.to_string_lossy();
                if is_windows_drive_component(&value) {
                    return Err(format!("unsafe RAR member path: {member_name}"));
                }
                safe.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!("unsafe RAR member path: {member_name}"));
            }
        }
    }

    if safe.as_os_str().is_empty() {
        return Err(format!("unsafe RAR member path: {member_name}"));
    }

    Ok(safe)
}

fn is_windows_drive_component(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

#[cfg(unix)]
fn current_umask() -> u32 {
    static UMASK: OnceLock<u32> = OnceLock::new();
    *UMASK.get_or_init(|| {
        let mask = unsafe { libc::umask(0o022) };
        unsafe {
            libc::umask(mask);
        }
        mask as u32
    })
}

#[cfg(unix)]
fn rar_member_unix_output_mode(member: &unrar_rs::MemberInfo) -> Option<u32> {
    match member.host_os {
        unrar_rs::HostOs::Unix | unrar_rs::HostOs::Darwin => {
            let mode = member.attributes.unix_mode() & 0o7777;
            (mode != 0).then_some(mode)
        }
        unrar_rs::HostOs::Windows => {
            let mode = if member.is_directory || member.attributes.is_directory_attr() {
                0o777
            } else if member.attributes.is_readonly() {
                0o444
            } else {
                0o666
            };
            Some(mode & !current_umask())
        }
        unrar_rs::HostOs::Unknown(_) => {
            let mode = if member.is_directory { 0o777 } else { 0o666 };
            Some(mode & !current_umask())
        }
    }
}

fn apply_rar_member_filesystem_metadata(
    member: &unrar_rs::MemberInfo,
    out_path: &Path,
) -> Result<(), String> {
    match (member.mtime, member.atime) {
        (Some(mtime), Some(atime)) => filetime::set_file_times(
            out_path,
            filetime::FileTime::from_system_time(atime),
            filetime::FileTime::from_system_time(mtime),
        )
        .map_err(|error| {
            format!(
                "failed to restore times for RAR member {} at {}: {error}",
                member.name,
                out_path.display()
            )
        })?,
        (Some(mtime), None) => {
            filetime::set_file_mtime(out_path, filetime::FileTime::from_system_time(mtime))
                .map_err(|error| {
                    format!(
                        "failed to restore mtime for RAR member {} at {}: {error}",
                        member.name,
                        out_path.display()
                    )
                })?;
        }
        (None, Some(atime)) => {
            filetime::set_file_atime(out_path, filetime::FileTime::from_system_time(atime))
                .map_err(|error| {
                    format!(
                        "failed to restore atime for RAR member {} at {}: {error}",
                        member.name,
                        out_path.display()
                    )
                })?;
        }
        (None, None) => {}
    }

    #[cfg(unix)]
    {
        if let Some(mode) = rar_member_unix_output_mode(member) {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = std::fs::metadata(out_path)
                .map_err(|error| {
                    format!(
                        "failed to read metadata for RAR member {} at {}: {error}",
                        member.name,
                        out_path.display()
                    )
                })?
                .permissions();
            permissions.set_mode(mode);
            std::fs::set_permissions(out_path, permissions).map_err(|error| {
                format!(
                    "failed to restore permissions for RAR member {} at {}: {error}",
                    member.name,
                    out_path.display()
                )
            })?;
        }
    }

    Ok(())
}

fn ensure_unique_sanitized_rar_member_paths(archive: &unrar_rs::RarArchive) -> Result<(), String> {
    let mut occupied = std::collections::HashSet::<String>::new();
    for raw_name in archive.started_member_names() {
        let member_name = unrar_rs::sanitize_path(raw_name);
        let safe_path = validate_sanitized_rar_member_path(&member_name)?;
        let collision_key = safe_path
            .to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase();
        if !occupied.insert(collision_key) {
            return Err(format!(
                "RAR archive contains colliding sanitized member path: {member_name}"
            ));
        }
    }
    Ok(())
}

impl<'a> RarExtractionContext<'a> {
    pub(crate) fn new(
        volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
        event_tx: &'a broadcast::Sender<PipelineEvent>,
        job_id: JobId,
        set_name: &'a str,
        output_dir: &'a std::path::Path,
        options: &'a unrar_rs::ExtractOptions,
    ) -> Self {
        Self {
            volume_paths,
            event_tx,
            job_id,
            set_name,
            output_dir,
            root: None,
            budget: None,
            options,
            phase_attempt: None,
        }
    }

    pub(crate) fn with_phase_attempt(mut self, attempt: Option<Arc<PhaseAttemptCounters>>) -> Self {
        self.phase_attempt = attempt;
        self
    }

    pub(crate) fn with_security(
        mut self,
        root: Arc<ExtractionRoot>,
        budget: Arc<JobExtractionBudget>,
    ) -> Self {
        self.root = Some(root);
        self.budget = Some(budget);
        self
    }
}

impl Pipeline {
    pub(crate) fn extract_rar_member_to_output(
        archive: &mut unrar_rs::RarArchive,
        ctx: RarExtractionContext<'_>,
        idx: usize,
    ) -> Result<(String, u64, u64), String> {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("rar.extract_member");
        let RarExtractionContext {
            volume_paths,
            event_tx,
            job_id,
            set_name,
            output_dir,
            root,
            budget,
            options,
            phase_attempt,
        } = ctx;
        let root = match root {
            Some(root) => root,
            None => Arc::new(ExtractionRoot::open(output_dir)?),
        };
        let budget = match budget {
            Some(budget) => budget,
            None => {
                let limits = Arc::new(ExtractionLimits::from_env(output_dir)?);
                let (initial_entries, initial_bytes) = ExtractionRoot::snapshot_usage(output_dir)?;
                JobExtractionBudget::new(
                    limits,
                    output_dir.to_path_buf(),
                    archive
                        .metadata()
                        .members
                        .iter()
                        .map(|member| member.compressed_size)
                        .sum(),
                    initial_entries,
                    initial_bytes,
                    PipelineMetrics::new(),
                )?
            }
        };
        let phase_guard = PhaseAttemptRollbackGuard::new(phase_attempt);
        let member = archive
            .member_info(idx)
            .ok_or_else(|| format!("member index {idx} missing from archive metadata"))?;
        let member_name = member.name.clone();
        let safe_member_path = validate_sanitized_rar_member_path(&member_name)
            .map_err(|error| budget.reject_unsafe_path(error))?;
        let unpacked_size = member.unpacked_size.unwrap_or(0);
        budget.check_member_metadata(&member_name, unpacked_size)?;
        let is_directory = member.is_directory;
        let first_volume = member.volumes.first_volume as u32;
        let last_volume = member.volumes.last_volume as u32;
        let is_solid = archive.is_solid();

        if is_directory {
            let dir_path = output_dir.join(&safe_member_path);
            root.create_dir(&safe_member_path, &budget)?;
            apply_rar_member_filesystem_metadata(&member, &dir_path)?;
            return Ok((member_name, 0, unpacked_size));
        }

        if member.is_symlink || member.is_hardlink || member.is_file_copy {
            return Err(budget.reject_unsupported_entry(format!(
                "RAR member '{member_name}' is a link or file-copy entry"
            )));
        }

        // Reserve this member's share of the Extracting total here, at open
        // time: the archive topology is often absent when the batch is
        // scheduled (it is only rebuilt from volume 0 afterwards), so a
        // scheduling-time reservation silently contributes nothing and the
        // phase never publishes a bar. The size is always known on the open
        // archive, and the total only grows — member by member — honoring the
        // hone-in rule. Rolled back with the attempt on failure, so a retry
        // reserving again does not double-count.
        phase_guard.reserve_total(unpacked_size);

        let safe_member_name = safe_member_path.to_string_lossy().replace('\\', "/");
        let (out_path, partial_path) = Self::member_output_paths(output_dir, &safe_member_name);

        let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
            job_id,
            set_name: set_name.to_string(),
            member: member_name.clone(),
        });
        crate::e2e_failpoint::maybe_delay("extract.member_start");

        let chunk_dir = Self::member_chunk_dir(output_dir, set_name, &member_name);

        let partial_size = std::fs::metadata(&partial_path).ok().map(|meta| meta.len());
        let out_size = std::fs::metadata(&out_path).ok().map(|meta| meta.len());
        info!(
            job_id = job_id.0,
            set_name,
            member = %member_name,
            idx,
            first_volume,
            last_volume,
            is_solid,
            available_volumes = ?volume_paths.keys().copied().collect::<Vec<_>>(),
            partial_exists = partial_path.exists(),
            partial_size,
            out_exists = out_path.exists(),
            out_size,
            "RAR member extraction begin"
        );

        if partial_path.exists() || chunk_dir.exists() {
            Self::clear_member_extraction_artifacts(&partial_path, &chunk_dir)?;
        }

        let partial_relative = partial_path.strip_prefix(output_dir).map_err(|error| {
            budget.reject_unsafe_path(format!("RAR partial output escaped staging root: {error}"))
        })?;
        let partial_file = root.create_file(partial_relative, &budget)?;
        let shared = Rc::new(RefCell::new(SharedOutputFile {
            inner: std::io::BufWriter::with_capacity(8 * 1024 * 1024, partial_file),
        }));
        let checkpoint = Arc::new(ExtractionCheckpointState {
            job_id,
            set_name: set_name.to_string(),
            member_name: member_name.clone(),
            temp_path: partial_path.to_string_lossy().to_string(),
            manifest: Mutex::new(Vec::new()),
            next_offset: AtomicU64::new(0),
            error: Mutex::new(None),
        });

        let chunk_records: Result<Vec<(u32, u64)>, unrar_rs::RarError> = if is_solid {
            let shared_ref = Rc::clone(&shared);
            let checkpoint_ref = Arc::clone(&checkpoint);
            archive
                .extract_member_solid_chunked(idx, options, |absolute_volume| {
                    let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                        unrar_rs::RarError::CorruptArchive {
                            detail: format!(
                                "solid chunk volume {absolute_volume} does not fit into u32"
                            ),
                        }
                    })?;
                    Ok(Box::new(DirectOutputWriter {
                        shared: Some(Rc::clone(&shared_ref)),
                        bytes_written: 0,
                        volume_index: absolute_volume,
                        checkpoint: Some(Arc::clone(&checkpoint_ref)),
                        phase_attempt: phase_guard.attempt(),
                    }) as Box<dyn Write>)
                })
                .and_then(|records| {
                    records
                        .into_iter()
                        .map(|(absolute_volume, bytes_written)| {
                            let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                                unrar_rs::RarError::CorruptArchive {
                                    detail: format!(
                                        "solid chunk volume {absolute_volume} does not fit into u32"
                                    ),
                                }
                            })?;
                            Ok((absolute_volume, bytes_written))
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
        } else {
            // Keyed by the set's own volume indices, which is the numbering
            // `extract_member_streaming_chunked` asks the provider for and
            // reports its chunks against.
            let mut provider_paths = std::collections::HashMap::new();
            for absolute_volume in first_volume..=last_volume {
                let Some(path) = volume_paths.get(&absolute_volume) else {
                    return Err(format!(
                        "missing local RAR volume {absolute_volume} for member {member_name}"
                    ));
                };
                provider_paths.insert(absolute_volume as usize, path.clone());
            }
            let provider = ReadaheadVolumeProvider::new(provider_paths);
            let provider = BudgetedRarVolumeProvider::new(&provider, Arc::clone(&budget));
            let shared_ref = Rc::clone(&shared);
            let checkpoint_ref = Arc::clone(&checkpoint);
            archive
                .extract_member_streaming_chunked(idx, options, &provider, |absolute_volume| {
                    let volume_index = u32::try_from(absolute_volume).map_err(|_| {
                        unrar_rs::RarError::CorruptArchive {
                            detail: format!("chunk volume {absolute_volume} does not fit into u32"),
                        }
                    })?;
                    Ok(Box::new(DirectOutputWriter {
                        shared: Some(Rc::clone(&shared_ref)),
                        bytes_written: 0,
                        volume_index,
                        checkpoint: Some(Arc::clone(&checkpoint_ref)),
                        phase_attempt: phase_guard.attempt(),
                    }) as Box<dyn Write>)
                })
                .and_then(|records| {
                    records
                        .into_iter()
                        .map(|(absolute_volume, bytes_written)| {
                            let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                                unrar_rs::RarError::CorruptArchive {
                                    detail: format!(
                                        "chunk volume {absolute_volume} does not fit into u32"
                                    ),
                                }
                            })?;
                            Ok((absolute_volume, bytes_written))
                        })
                        .collect::<Result<Vec<_>, unrar_rs::RarError>>()
                })
        };
        let chunk_records = chunk_records.map_err(|error| {
            let _ = std::fs::remove_file(&partial_path);
            format!("failed to extract {member_name}: {error}")
        })?;
        let chunk_total = chunk_records
            .iter()
            .map(|(_, bytes_written)| *bytes_written)
            .sum::<u64>();
        let partial_size_after_extract =
            std::fs::metadata(&partial_path).ok().map(|meta| meta.len());
        info!(
            job_id = job_id.0,
            set_name,
            member = %member_name,
            chunk_records = ?chunk_records,
            chunk_total,
            unpacked_size,
            partial_size_after_extract,
            "RAR member extraction produced chunk records"
        );
        if chunk_total != unpacked_size {
            warn!(
                job_id = job_id.0,
                set_name,
                member = %member_name,
                chunk_total,
                unpacked_size,
                "RAR member chunk records do not sum to unpacked size"
            );
        }

        if let Some(error) = checkpoint.take_error() {
            let _ = std::fs::remove_file(&partial_path);
            return Err(error);
        }

        {
            let mut shared_file = shared.borrow_mut();
            shared_file.inner.flush().map_err(|e| {
                format!(
                    "failed to flush partial output {}: {e}",
                    partial_path.display()
                )
            })?;
            shared_file.inner.get_ref().sync_all().map_err(|e| {
                format!(
                    "failed to sync partial output {}: {e}",
                    partial_path.display()
                )
            })?;
        }
        drop(shared);

        let bytes_written = match Self::finalize_member_output(FinalizeMemberContext {
            event_tx,
            job_id,
            set_name,
            member_name: &member_name,
            partial_path: &partial_path,
            out_path: &out_path,
            chunk_dir: &chunk_dir,
        }) {
            Ok(bytes_written) => bytes_written,
            Err(error) => {
                let _ = std::fs::remove_file(&partial_path);
                return Err(error);
            }
        };
        apply_rar_member_filesystem_metadata(&member, &out_path)?;
        info!(
            job_id = job_id.0,
            set_name,
            member = %member_name,
            bytes_written,
            unpacked_size,
            out_path = %out_path.display(),
            "RAR member extraction finalized"
        );

        let _ = chunk_records;

        phase_guard.commit();
        Ok((member_name, bytes_written, unpacked_size))
    }

    pub(crate) fn open_rar_archive_from_snapshot_or_disk(
        request: RarArchiveSnapshotOpenRequest<'_>,
    ) -> Result<crate::pipeline::ArchivePasswordSelection<unrar_rs::RarArchive>, String> {
        let RarArchiveSnapshotOpenRequest {
            set_name,
            volume_paths,
            password_candidates,
            cached_headers,
            shared_kdf_cache,
            open_mode,
            requested_members,
            already_extracted,
            budget,
        } = request;
        let context = format!("failed to open RAR archive for set '{set_name}'");
        let inputs = RarArchiveOpenInputs {
            set_name,
            volume_paths: &volume_paths,
            cached_headers: cached_headers.as_deref(),
            shared_kdf_cache,
            open_mode,
            requested_members,
            already_extracted,
            budget,
        };
        Self::try_rar_password_candidates(&context, &password_candidates, |password| {
            Self::open_rar_archive_from_snapshot_or_disk_with_password(&inputs, password)
        })
        .and_then(|selection| {
            ensure_unique_sanitized_rar_member_paths(&selection.value)?;
            Ok(selection)
        })
    }

    pub(crate) fn open_rar_archive_for_extraction_with_password_candidates(
        request: RarExtractionOpenRequest<'_>,
    ) -> Result<RarExtractionOpenSelection, String> {
        let RarExtractionOpenRequest {
            set_name,
            volume_paths,
            password_candidates,
            cached_headers,
            shared_kdf_cache,
            open_mode,
            requested_members,
            already_extracted,
            budget,
        } = request;

        if password_candidates.len() <= 1 {
            let selection =
                Self::open_rar_archive_from_snapshot_or_disk(RarArchiveSnapshotOpenRequest {
                    set_name,
                    volume_paths,
                    password_candidates,
                    cached_headers,
                    shared_kdf_cache,
                    open_mode,
                    requested_members: Some(requested_members),
                    already_extracted,
                    budget,
                })?;
            let decoder_memory_bytes = rar_decoder_memory_bytes(&selection.value);
            return Ok(RarExtractionOpenSelection {
                archive: selection.value,
                password: selection.selected_password,
                validated_password: None,
                decoder_memory_bytes,
            });
        }

        let context = format!("failed to validate RAR password for set '{set_name}'");
        let inputs = RarArchiveOpenInputs {
            set_name,
            volume_paths: &volume_paths,
            cached_headers: cached_headers.as_deref(),
            shared_kdf_cache,
            open_mode,
            requested_members: Some(requested_members),
            already_extracted,
            budget: budget.clone(),
        };
        let selection =
            Self::try_rar_password_candidates(&context, &password_candidates, |password| {
                let mut probe_archive =
                    Self::open_rar_archive_from_snapshot_or_disk_with_password(&inputs, password)?;
                let _memory_permit = if let Some(budget) = inputs.budget.as_ref() {
                    let required = rar_decoder_memory_bytes(&probe_archive);
                    Some(
                        budget
                            .reserve_memory_wait(required)
                            .map_err(crate::pipeline::RarPasswordAttemptError::Fatal)?,
                    )
                } else {
                    None
                };
                let probe = Self::select_rar_password_probe_member(
                    &probe_archive,
                    requested_members,
                    already_extracted,
                );
                let password_validated = if let Some((idx, requires_password)) = probe {
                    Self::probe_rar_member_password(
                        &mut probe_archive,
                        &volume_paths,
                        idx,
                        password,
                        inputs.budget.as_ref(),
                    )?;
                    requires_password
                } else {
                    false
                };
                let archive =
                    Self::open_rar_archive_from_snapshot_or_disk_with_password(&inputs, password)?;
                Ok((archive, password_validated))
            })?;
        let (archive, password_validated) = selection.value;
        ensure_unique_sanitized_rar_member_paths(&archive)?;
        let decoder_memory_bytes = rar_decoder_memory_bytes(&archive);
        let password = selection.selected_password;
        let validated_password = password_validated.then(|| password.clone()).flatten();
        Ok(RarExtractionOpenSelection {
            archive,
            password,
            validated_password,
            decoder_memory_bytes,
        })
    }

    fn archive_needs_attached_source_readers(
        archive: &unrar_rs::RarArchive,
        requested_members: Option<&[String]>,
        already_extracted: Option<&std::collections::HashSet<String>>,
    ) -> bool {
        if archive.is_solid() {
            return true;
        }

        let member_needs_attached_reader =
            |info: unrar_rs::MemberInfo| info.is_symlink || info.is_hardlink || info.is_file_copy;

        requested_members.is_some_and(|members| {
            if members.is_empty() {
                archive.metadata().members.iter().any(|member| {
                    !already_extracted.is_some_and(|extracted| extracted.contains(&member.name))
                        && member_needs_attached_reader(member.clone())
                })
            } else {
                members.iter().any(|member| {
                    archive
                        .find_member_sanitized(member)
                        .and_then(|idx| archive.member_info(idx))
                        .is_some_and(member_needs_attached_reader)
                })
            }
        })
    }

    fn open_rar_archive_from_snapshot_or_disk_with_password(
        inputs: &RarArchiveOpenInputs<'_>,
        password: Option<&str>,
    ) -> Result<unrar_rs::RarArchive, crate::pipeline::RarPasswordAttemptError> {
        let set_name = inputs.set_name;
        let volume_paths = inputs.volume_paths;
        let cached_headers = inputs.cached_headers;
        let open_mode = inputs.open_mode;
        let requested_members = inputs.requested_members;
        let already_extracted = inputs.already_extracted;
        if let Some(budget) = inputs.budget.as_ref() {
            budget.check_active_io().map_err(|error| {
                crate::pipeline::RarPasswordAttemptError::Fatal(error.to_string())
            })?;
        }
        let has_cached_headers = cached_headers.is_some();
        let refresh_provided_volumes =
            matches!(open_mode, RarArchiveOpenMode::RefreshProvidedVolumes);

        let mut archive = match cached_headers {
            Some(headers) => {
                if let Some(first_path) = volume_paths.get(&0) {
                    let _ = Self::open_rar_volume_zero_with_password(
                        first_path,
                        password,
                        inputs.shared_kdf_cache.clone(),
                        inputs.budget.as_ref(),
                    )?;
                }
                unrar_rs::RarArchive::deserialize_headers_with_password_and_shared_kdf_cache(
                    headers,
                    password.map(str::to_string),
                    inputs.shared_kdf_cache.clone(),
                )
                .map_err(|error| {
                    crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                        "failed to deserialize cached RAR headers for set '{set_name}': {error}"
                    ))
                })?
            }
            None => {
                let first_path = volume_paths.get(&0).ok_or_else(|| {
                    crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                        "RAR set '{set_name}' cannot be opened without volume 0"
                    ))
                })?;
                Self::open_rar_volume_zero_with_password(
                    first_path,
                    password,
                    inputs.shared_kdf_cache.clone(),
                    inputs.budget.as_ref(),
                )?
            }
        };
        apply_server_rar_limits_with_memory_limit(
            &mut archive,
            inputs
                .budget
                .as_ref()
                .map(|budget| budget.max_memory_bytes())
                .unwrap_or(u64::MAX),
        );

        let full_set_open = requested_members.is_some_and(|members| members.is_empty());
        let metadata_may_expand =
            full_set_open && (!has_cached_headers || refresh_provided_volumes);
        let retain_attached_readers = metadata_may_expand
            || Self::archive_needs_attached_source_readers(
                &archive,
                requested_members,
                already_extracted,
            );
        let bounded_sources = retain_attached_readers.then(BoundedRarSourcePool::single_fd);

        if has_cached_headers && !refresh_provided_volumes && !retain_attached_readers {
            return Ok(archive);
        }

        for (volume_number, path) in volume_paths {
            if archive.has_volume(*volume_number as usize)
                && !refresh_provided_volumes
                && retain_attached_readers
            {
                archive.attach_volume_reader(
                    *volume_number as usize,
                    if let Some(budget) = inputs.budget.as_ref() {
                        Box::new(BudgetedReader::new(
                            bounded_sources
                                .as_ref()
                                .expect("bounded source pool should exist")
                                .reader(path.clone()),
                            Arc::clone(budget),
                        ))
                    } else {
                        bounded_sources
                            .as_ref()
                            .expect("bounded source pool should exist")
                            .reader(path.clone())
                    },
                );
                continue;
            }

            if archive.has_volume(*volume_number as usize)
                && !refresh_provided_volumes
                && !retain_attached_readers
            {
                archive.attach_volume_reader(
                    *volume_number as usize,
                    Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                );
                continue;
            }

            let file = match std::fs::File::open(path) {
                Ok(file) => file,
                Err(error)
                    if has_cached_headers && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    continue;
                }
                Err(error) => {
                    let context =
                        format!("failed to open RAR volume {volume_number} for set '{set_name}'");
                    return Err(crate::pipeline::RarPasswordAttemptError::Fatal(
                        crate::pipeline::capacity::format_fd_capacity_error(&context, &error),
                    ));
                }
            };
            let file: Box<dyn unrar_rs::ReadSeek> = if let Some(budget) = inputs.budget.as_ref() {
                Box::new(BudgetedReader::new(file, Arc::clone(budget)))
            } else {
                Box::new(file)
            };
            if has_cached_headers
                && refresh_provided_volumes
                && archive.has_volume(*volume_number as usize)
            {
                archive
                    .refresh_volume(*volume_number as usize, file)
                    .map_err(|error| {
                        crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                            "failed to refresh RAR volume {volume_number} for set '{set_name}': {error}"
                        ))
                    })?;
            } else if archive.has_volume(*volume_number as usize) {
                archive.attach_volume_reader(*volume_number as usize, file);
            } else {
                archive
                    .add_volume(*volume_number as usize, file)
                    .map_err(crate::pipeline::RarPasswordAttemptError::from)?;
            }
            if retain_attached_readers {
                archive.attach_volume_reader(
                    *volume_number as usize,
                    if let Some(budget) = inputs.budget.as_ref() {
                        Box::new(BudgetedReader::new(
                            bounded_sources
                                .as_ref()
                                .expect("bounded source pool should exist")
                                .reader(path.clone()),
                            Arc::clone(budget),
                        ))
                    } else {
                        bounded_sources
                            .as_ref()
                            .expect("bounded source pool should exist")
                            .reader(path.clone())
                    },
                );
            } else {
                archive.attach_volume_reader(
                    *volume_number as usize,
                    Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                );
            }
        }

        Ok(archive)
    }

    fn open_rar_volume_zero_with_password(
        first_path: &PathBuf,
        password: Option<&str>,
        shared_kdf_cache: std::sync::Arc<unrar_rs::crypto::KdfCache>,
        budget: Option<&Arc<JobExtractionBudget>>,
    ) -> Result<unrar_rs::RarArchive, crate::pipeline::RarPasswordAttemptError> {
        let first_file = std::fs::File::open(first_path).map_err(|e| {
            crate::pipeline::RarPasswordAttemptError::Fatal(
                crate::pipeline::capacity::format_fd_capacity_error(
                    "failed to open RAR volume 0",
                    &e,
                ),
            )
        })?;
        let first_file: Box<dyn unrar_rs::ReadSeek> = if let Some(budget) = budget {
            Box::new(BudgetedReader::new(first_file, Arc::clone(budget)))
        } else {
            Box::new(first_file)
        };
        match password {
            Some(password) => unrar_rs::RarArchive::open_with_password_and_shared_kdf_cache(
                first_file,
                password,
                shared_kdf_cache,
            ),
            None => unrar_rs::RarArchive::open_with_shared_kdf_cache(first_file, shared_kdf_cache),
        }
        .map_err(crate::pipeline::RarPasswordAttemptError::from)
    }

    fn select_rar_password_probe_member(
        archive: &unrar_rs::RarArchive,
        requested_members: &[String],
        already_extracted: Option<&std::collections::HashSet<String>>,
    ) -> Option<(usize, bool)> {
        let metadata = archive.metadata();
        let mut candidates = Vec::new();
        if requested_members.is_empty() {
            for (idx, member) in metadata.members.iter().enumerate() {
                if member.is_directory
                    || already_extracted.is_some_and(|extracted| extracted.contains(&member.name))
                {
                    continue;
                }
                candidates.push((idx, metadata.is_encrypted || member.is_encrypted));
            }
        } else {
            for requested in requested_members {
                let Some((idx, member)) = metadata
                    .members
                    .iter()
                    .enumerate()
                    .find(|(_, member)| member.name == *requested && !member.is_directory)
                else {
                    continue;
                };
                candidates.push((idx, metadata.is_encrypted || member.is_encrypted));
            }
        }

        candidates
            .iter()
            .copied()
            .find(|(idx, _)| metadata.members[*idx].is_encrypted)
            .or_else(|| candidates.first().copied())
    }

    fn probe_rar_member_password(
        archive: &mut unrar_rs::RarArchive,
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
        idx: usize,
        password: Option<&str>,
        budget: Option<&Arc<JobExtractionBudget>>,
    ) -> Result<(), crate::pipeline::RarPasswordAttemptError> {
        let member = archive.member_info(idx).ok_or_else(|| {
            crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                "member index {idx} missing from archive metadata"
            ))
        })?;
        if member.is_directory {
            return Ok(());
        }
        let options = unrar_rs::ExtractOptions {
            verify: true,
            password: password.map(str::to_string),
            restore_owners: false,
        };

        if archive.is_solid() {
            archive
                .extract_member_solid_chunked(idx, &options, |_| {
                    Ok(Box::new(std::io::sink()) as Box<dyn Write>)
                })
                .map(|_| ())
                .map_err(crate::pipeline::RarPasswordAttemptError::from)?;
            return Ok(());
        }

        let first_volume = member.volumes.first_volume as u32;
        let last_volume = member.volumes.last_volume as u32;
        // Keyed by the set's own volume indices: `extract_member_streaming`
        // asks for the volumes the member's segments name.
        let mut provider_paths = std::collections::HashMap::new();
        for absolute_volume in first_volume..=last_volume {
            let Some(path) = volume_paths.get(&absolute_volume) else {
                return Err(crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                    "missing local RAR volume {absolute_volume} for member {}",
                    member.name
                )));
            };
            provider_paths.insert(absolute_volume as usize, path.clone());
        }
        let provider = ReadaheadVolumeProvider::new(provider_paths);
        let budgeted_provider =
            budget.map(|budget| BudgetedRarVolumeProvider::new(&provider, Arc::clone(budget)));
        let mut sink = std::io::sink();
        archive
            .extract_member_streaming(
                idx,
                &options,
                budgeted_provider
                    .as_ref()
                    .map(|provider| provider as &dyn unrar_rs::VolumeProvider)
                    .unwrap_or(&provider),
                &mut sink,
            )
            .map(|_| ())
            .map_err(crate::pipeline::RarPasswordAttemptError::from)
    }

    pub(crate) fn try_rar_password_candidates<T, F>(
        context: &str,
        candidates: &[crate::jobs::ArchivePasswordCandidate],
        mut attempt: F,
    ) -> Result<crate::pipeline::ArchivePasswordSelection<T>, String>
    where
        F: FnMut(Option<&str>) -> Result<T, crate::pipeline::RarPasswordAttemptError>,
    {
        if candidates.is_empty() {
            return attempt(None)
                .map(|value| crate::pipeline::ArchivePasswordSelection::new(value, None))
                .map_err(|error| format!("{context}: {error}"));
        }

        let mut last_password_error = None;
        for candidate in candidates {
            match attempt(Some(candidate.value())) {
                Ok(value) => {
                    return Ok(crate::pipeline::ArchivePasswordSelection::new(
                        value,
                        Some(candidate.value().to_string()),
                    ));
                }
                Err(crate::pipeline::RarPasswordAttemptError::Rar(error))
                    if Self::rar_error_is_password_related(&error) =>
                {
                    last_password_error = Some(error);
                }
                Err(error) => return Err(format!("{context}: {error}")),
            }
        }

        let sources = Self::password_candidate_sources(candidates);
        Err(format!(
            "{context}: invalid password for encrypted archive after {} candidate(s) from {sources}: {}",
            candidates.len(),
            last_password_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "password rejected".to_string())
        ))
    }

    pub(crate) fn deserialize_rar_headers_with_password_candidates(
        set_name: &str,
        headers: &[u8],
        candidates: &[crate::jobs::ArchivePasswordCandidate],
        shared_kdf_cache: std::sync::Arc<unrar_rs::crypto::KdfCache>,
    ) -> Result<crate::pipeline::ArchivePasswordSelection<unrar_rs::RarArchive>, String> {
        let selected = candidates.first();
        let password = selected.map(|candidate| candidate.value().to_string());
        unrar_rs::RarArchive::deserialize_headers_with_password_and_shared_kdf_cache(
            headers,
            password,
            shared_kdf_cache,
        )
        .map(|archive| {
            crate::pipeline::ArchivePasswordSelection::new(
                archive,
                selected.map(|candidate| candidate.value().to_string()),
            )
        })
        .map_err(|error| {
            format!("failed to deserialize cached RAR headers for set '{set_name}': {error}")
        })
    }

    pub(crate) fn open_rar_volume_zero_with_password_candidates(
        set_name: &str,
        first_path: &PathBuf,
        candidates: &[crate::jobs::ArchivePasswordCandidate],
        shared_kdf_cache: std::sync::Arc<unrar_rs::crypto::KdfCache>,
    ) -> Result<crate::pipeline::ArchivePasswordSelection<unrar_rs::RarArchive>, String> {
        let context = format!("failed to parse RAR volume 0 for set '{set_name}'");
        Self::try_rar_password_candidates(&context, candidates, |password| {
            Self::open_rar_volume_zero_with_password(
                first_path,
                password,
                shared_kdf_cache.clone(),
                None,
            )
        })
    }

    pub(crate) fn rar_error_is_password_related(error: &unrar_rs::RarError) -> bool {
        matches!(
            error,
            unrar_rs::RarError::EncryptedArchive
                | unrar_rs::RarError::EncryptedMember { .. }
                | unrar_rs::RarError::InvalidPassword
                | unrar_rs::RarError::WrongPassword { .. }
        )
    }

    pub(crate) fn password_candidate_sources(
        candidates: &[crate::jobs::ArchivePasswordCandidate],
    ) -> String {
        candidates
            .iter()
            .map(|candidate| candidate.source().as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn metadata_test_member(
        name: &str,
        host_os: unrar_rs::HostOs,
        attributes: u64,
        is_directory: bool,
        mtime_secs: Option<u64>,
    ) -> unrar_rs::MemberInfo {
        unrar_rs::MemberInfo {
            name: name.to_string(),
            raw_name: name.to_string(),
            raw_name_bytes: Some(name.as_bytes().to_vec()),
            unpacked_size: Some(0),
            compressed_size: 0,
            is_directory,
            crc32: None,
            mtime: mtime_secs.map(|secs| UNIX_EPOCH + Duration::from_secs(secs)),
            ctime: None,
            atime: None,
            version: None,
            host_os,
            compression: unrar_rs::CompressionInfo {
                format: unrar_rs::ArchiveFormat::Rar5,
                version: 0,
                method: unrar_rs::CompressionMethod::Store,
                solid: false,
                dict_size: 0,
            },
            is_encrypted: false,
            hash: None,
            attributes: unrar_rs::types::FileAttributes(attributes),
            owner: None,
            volumes: unrar_rs::VolumeSpan {
                first_volume: 0,
                last_volume: 0,
            },
            is_symlink: false,
            is_hardlink: false,
            is_file_copy: false,
            link_target: None,
            link_target_bytes: None,
        }
    }

    #[test]
    fn sanitized_rar_member_path_rejects_empty_and_parent_components() {
        assert!(validate_sanitized_rar_member_path("").is_err());
        assert!(validate_sanitized_rar_member_path("../escape.txt").is_err());
        assert!(validate_sanitized_rar_member_path("nested/../../escape.txt").is_err());
    }

    #[test]
    fn sanitized_rar_member_path_rejects_absolute_and_drive_paths() {
        assert!(validate_sanitized_rar_member_path("/absolute.txt").is_err());
        assert!(validate_sanitized_rar_member_path("C:/windows.txt").is_err());
    }

    #[test]
    fn sanitized_rar_member_path_accepts_nested_relative_paths() {
        let path = validate_sanitized_rar_member_path("nested/movie.mkv").unwrap();

        assert_eq!(path, PathBuf::from("nested").join("movie.mkv"));
    }

    #[test]
    fn rar_member_metadata_restores_mtime_and_unix_mode() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("movie.mkv");
        std::fs::write(&path, b"payload").unwrap();
        let member = metadata_test_member(
            "movie.mkv",
            unrar_rs::HostOs::Unix,
            0o640,
            false,
            Some(1_700_000_123),
        );

        apply_rar_member_filesystem_metadata(&member, &path).unwrap();

        let metadata = std::fs::metadata(&path).unwrap();
        let actual_mtime = filetime::FileTime::from_last_modification_time(&metadata);
        assert_eq!(actual_mtime.unix_seconds(), 1_700_000_123);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(metadata.permissions().mode() & 0o7777, 0o640);
        }
    }

    #[cfg(unix)]
    #[test]
    fn rar_member_mode_translation_matches_unrar_attribute_rules() {
        let readonly_file =
            metadata_test_member("readonly.txt", unrar_rs::HostOs::Windows, 0x1, false, None);
        assert_eq!(
            rar_member_unix_output_mode(&readonly_file),
            Some(0o444 & !current_umask())
        );

        let windows_dir = metadata_test_member("dir", unrar_rs::HostOs::Windows, 0x10, true, None);
        assert_eq!(
            rar_member_unix_output_mode(&windows_dir),
            Some(0o777 & !current_umask())
        );

        let unix_without_mode =
            metadata_test_member("empty-mode", unrar_rs::HostOs::Unix, 0, false, None);
        assert_eq!(rar_member_unix_output_mode(&unix_without_mode), None);

        let darwin_mode = metadata_test_member(
            "darwin-mode",
            unrar_rs::HostOs::Darwin,
            0o100640,
            false,
            None,
        );
        assert_eq!(rar_member_unix_output_mode(&darwin_mode), Some(0o640));
    }

    #[test]
    #[cfg(unix)]
    fn rar_server_extraction_rejects_symlink_entries() {
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar5/rar5_symlink.rar");
        let file = std::fs::File::open(&fixture).unwrap();
        let mut archive = unrar_rs::RarArchive::open(file).unwrap();
        let symlink_idx = archive
            .metadata()
            .members
            .iter()
            .position(|member| member.is_symlink)
            .expect("fixture should contain a symlink member");
        let member = archive.member_info(symlink_idx).unwrap();
        let member_name = member.name.clone();
        let output_dir = tempfile::tempdir().unwrap();
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(8);
        let volume_paths = std::collections::BTreeMap::new();
        let options = unrar_rs::ExtractOptions::default();

        let error = Pipeline::extract_rar_member_to_output(
            &mut archive,
            RarExtractionContext {
                volume_paths: &volume_paths,
                event_tx: &event_tx,
                job_id: JobId(42),
                set_name: "rar5-symlink",
                output_dir: output_dir.path(),
                root: None,
                budget: None,
                options: &options,
                phase_attempt: None,
            },
            symlink_idx,
        )
        .unwrap_err();

        assert!(error.contains("unsupported_entry"));
        assert!(!output_dir.path().join(member_name).exists());
    }

    #[test]
    #[cfg(unix)]
    fn full_set_reader_selection_scans_link_members_when_requested_members_is_empty() {
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar5/rar5_symlink.rar");
        let file = std::fs::File::open(&fixture).unwrap();
        let archive = unrar_rs::RarArchive::open(file).unwrap();
        let symlink_name = archive
            .metadata()
            .members
            .iter()
            .find(|member| member.is_symlink)
            .expect("fixture should contain a symlink member")
            .name
            .clone();
        let requested_members: Vec<String> = Vec::new();

        assert!(Pipeline::archive_needs_attached_source_readers(
            &archive,
            Some(&requested_members),
            None,
        ));

        let already_extracted = std::collections::HashSet::from([symlink_name]);
        assert!(!Pipeline::archive_needs_attached_source_readers(
            &archive,
            Some(&requested_members),
            Some(&already_extracted),
        ));
    }

    const TEST_RAR5_SIG: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

    fn encode_test_rar_vint(mut value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            result.push(byte);
            if value == 0 {
                break;
            }
        }
        result
    }

    fn build_test_rar_header(
        header_type: u64,
        common_flags: u64,
        type_body: &[u8],
        extra: &[u8],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_test_rar_vint(header_type));
        let mut flags = common_flags;
        if !extra.is_empty() {
            flags |= 0x0001;
        }
        body.extend_from_slice(&encode_test_rar_vint(flags));
        if !extra.is_empty() {
            body.extend_from_slice(&encode_test_rar_vint(extra.len() as u64));
        }
        body.extend_from_slice(type_body);
        body.extend_from_slice(extra);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_test_rar_vint(header_size);
        let crc =
            par2_rs::checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());
        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    fn build_test_rar_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(archive_flags));
        if let Some(volume_number) = volume_number {
            type_body.extend_from_slice(&encode_test_rar_vint(volume_number));
        }
        build_test_rar_header(1, 0, &type_body, &[])
    }

    fn build_test_rar_end_header(more_volumes: bool) -> Vec<u8> {
        let end_flags: u64 = if more_volumes { 0x0001 } else { 0 };
        build_test_rar_header(5, 0, &encode_test_rar_vint(end_flags), &[])
    }

    fn build_test_rar_file_header(
        filename: &str,
        common_flags_extra: u64,
        compression_info: u64,
        data_size: u64,
        unpacked_size: u64,
        data_crc: Option<u32>,
    ) -> Vec<u8> {
        let file_flags: u64 = if data_crc.is_some() { 0x0004 } else { 0 };
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(file_flags));
        type_body.extend_from_slice(&encode_test_rar_vint(unpacked_size));
        type_body.extend_from_slice(&encode_test_rar_vint(0o644));
        if let Some(data_crc) = data_crc {
            type_body.extend_from_slice(&data_crc.to_le_bytes());
        }
        type_body.extend_from_slice(&encode_test_rar_vint(compression_info));
        type_body.extend_from_slice(&encode_test_rar_vint(1));
        type_body.extend_from_slice(&encode_test_rar_vint(filename.len() as u64));
        type_body.extend_from_slice(filename.as_bytes());

        let mut body = Vec::new();
        body.extend_from_slice(&encode_test_rar_vint(2));
        body.extend_from_slice(&encode_test_rar_vint(0x0002 | common_flags_extra));
        body.extend_from_slice(&encode_test_rar_vint(data_size));
        body.extend_from_slice(&type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_test_rar_vint(header_size);
        let crc =
            par2_rs::checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());
        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    fn build_solid_store_multivolume_rar_set(volume_count: usize) -> Vec<(String, Vec<u8>)> {
        assert!(volume_count >= 2);
        let filename = "big.bin";
        let payload = (0..volume_count)
            .map(|index| b'a' + (index % 26) as u8)
            .collect::<Vec<_>>();
        let payload_crc = par2_rs::checksum::crc32(&payload);
        let solid_store_compression = 1u64 << 6;

        (0..volume_count)
            .map(|volume| {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&TEST_RAR5_SIG);
                bytes.extend_from_slice(&build_test_rar_main_header(
                    if volume == 0 {
                        0x0001 | 0x0004
                    } else {
                        0x0001 | 0x0002 | 0x0004
                    },
                    (volume > 0).then_some(volume as u64),
                ));
                let split_flags = match volume {
                    0 => 0x0010,
                    v if v + 1 == volume_count => 0x0008,
                    _ => 0x0010 | 0x0008,
                };
                bytes.extend_from_slice(&build_test_rar_file_header(
                    filename,
                    split_flags,
                    solid_store_compression,
                    1,
                    payload.len() as u64,
                    (volume + 1 == volume_count).then_some(payload_crc),
                ));
                bytes.push(payload[volume]);
                bytes.extend_from_slice(&build_test_rar_end_header(volume + 1 != volume_count));
                (format!("solid.part{volume:03}.rar"), bytes)
            })
            .collect()
    }

    #[test]
    fn solid_extraction_uses_bounded_source_readers_across_many_volumes() {
        let temp = tempfile::tempdir().unwrap();
        let volume_count = 260usize;
        let files = build_solid_store_multivolume_rar_set(volume_count);
        let first_archive =
            unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
        assert!(first_archive.is_solid());
        let cached_headers = first_archive.serialize_headers();

        let volume_paths = files
            .iter()
            .enumerate()
            .map(|(volume, (filename, bytes))| {
                let path = temp.path().join(filename);
                std::fs::write(&path, bytes).unwrap();
                (volume as u32, path)
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        let requested = vec!["big.bin".to_string()];
        let mut archive =
            Pipeline::open_rar_archive_from_snapshot_or_disk(RarArchiveSnapshotOpenRequest {
                set_name: "solid",
                volume_paths: volume_paths.clone(),
                password_candidates: Vec::new(),
                cached_headers: Some(cached_headers),
                shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
                open_mode: RarArchiveOpenMode::AttachOnly,
                requested_members: Some(&requested),
                already_extracted: None,
                budget: None,
            })
            .unwrap()
            .value;
        assert!(archive.is_solid());

        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&output_dir).unwrap();
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(8);
        let options = unrar_rs::ExtractOptions {
            verify: true,
            password: None,
            restore_owners: false,
        };
        let member_names = archive.member_names();
        let idx = archive
            .find_member_sanitized("big.bin")
            .unwrap_or_else(|| panic!("missing big.bin in decoded members: {member_names:?}"));

        super::super::source::reset_global_peak_open_count();
        let (name, written, unpacked) = Pipeline::extract_rar_member_to_output(
            &mut archive,
            RarExtractionContext::new(
                &volume_paths,
                &event_tx,
                JobId(77),
                "solid",
                &output_dir,
                &options,
            ),
            idx,
        )
        .unwrap();

        assert_eq!(name, "big.bin");
        assert_eq!(written, volume_count as u64);
        assert_eq!(unpacked, volume_count as u64);
        assert_eq!(
            std::fs::read(output_dir.join("big.bin")).unwrap().len(),
            volume_count
        );
        assert_eq!(super::super::source::global_peak_open_count(), 1);
    }
}
