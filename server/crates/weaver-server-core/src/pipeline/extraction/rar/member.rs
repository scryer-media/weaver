use super::checkpoint::{
    DirectOutputWriter, ExtractionCheckpointState, FinalizeMemberContext, SharedOutputFile,
};
use super::*;
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

pub(crate) struct RarExtractionContext<'a> {
    pub(crate) volume_paths: &'a std::collections::BTreeMap<u32, PathBuf>,
    pub(crate) event_tx: &'a broadcast::Sender<PipelineEvent>,
    pub(crate) job_id: JobId,
    pub(crate) set_name: &'a str,
    pub(crate) output_dir: &'a std::path::Path,
    pub(crate) options: &'a weaver_unrar::ExtractOptions,
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
    pub(crate) shared_kdf_cache: std::sync::Arc<weaver_unrar::crypto::KdfCache>,
    pub(crate) open_mode: RarArchiveOpenMode,
    pub(crate) requested_members: &'a [String],
    pub(crate) already_extracted: Option<&'a std::collections::HashSet<String>>,
}

pub(crate) struct RarExtractionOpenSelection {
    pub(crate) archive: weaver_unrar::RarArchive,
    pub(crate) password: Option<String>,
    pub(crate) validated_password: Option<String>,
}

fn validate_sanitized_rar_member_path(member_name: &str) -> Result<PathBuf, String> {
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

#[cfg(not(unix))]
fn current_umask() -> u32 {
    0
}

fn rar_member_unix_output_mode(member: &weaver_unrar::MemberInfo) -> Option<u32> {
    match member.host_os {
        weaver_unrar::HostOs::Unix | weaver_unrar::HostOs::Darwin => {
            let mode = member.attributes.unix_mode() & 0o7777;
            (mode != 0).then_some(mode)
        }
        weaver_unrar::HostOs::Windows => {
            let mode = if member.is_directory || member.attributes.is_directory_attr() {
                0o777
            } else if member.attributes.is_readonly() {
                0o444
            } else {
                0o666
            };
            Some(mode & !current_umask())
        }
        weaver_unrar::HostOs::Unknown(_) => {
            let mode = if member.is_directory { 0o777 } else { 0o666 };
            Some(mode & !current_umask())
        }
    }
}

fn apply_rar_member_filesystem_metadata(
    member: &weaver_unrar::MemberInfo,
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

fn ensure_unique_sanitized_rar_member_paths(
    archive: &weaver_unrar::RarArchive,
) -> Result<(), String> {
    let mut occupied = std::collections::HashSet::<String>::new();
    for raw_name in archive.member_names() {
        let member_name = weaver_unrar::sanitize_path(raw_name);
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
        options: &'a weaver_unrar::ExtractOptions,
    ) -> Self {
        Self {
            volume_paths,
            event_tx,
            job_id,
            set_name,
            output_dir,
            options,
        }
    }
}

impl Pipeline {
    pub(crate) fn extract_rar_member_to_output(
        archive: &mut weaver_unrar::RarArchive,
        ctx: RarExtractionContext<'_>,
        idx: usize,
    ) -> Result<(String, u64, u64), String> {
        let RarExtractionContext {
            volume_paths,
            event_tx,
            job_id,
            set_name,
            output_dir,
            options,
        } = ctx;
        let member = archive
            .member_info(idx)
            .ok_or_else(|| format!("member index {idx} missing from archive metadata"))?;
        let member_name = member.name.clone();
        let safe_member_path = validate_sanitized_rar_member_path(&member_name)?;
        let unpacked_size = member.unpacked_size.unwrap_or(0);
        let is_directory = member.is_directory;
        let first_volume = member.volumes.first_volume as u32;
        let last_volume = member.volumes.last_volume as u32;
        let is_solid = archive.is_solid();

        if is_directory {
            let dir_path = output_dir.join(&safe_member_path);
            std::fs::create_dir_all(&dir_path)
                .map_err(|e| format!("failed to create dir {}: {e}", member_name))?;
            apply_rar_member_filesystem_metadata(&member, &dir_path)?;
            return Ok((member_name, 0, unpacked_size));
        }

        let safe_member_name = safe_member_path.to_string_lossy().replace('\\', "/");
        let (out_path, partial_path) = Self::member_output_paths(output_dir, &safe_member_name);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create parent dir: {e}"))?;
        }

        let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
            job_id,
            set_name: set_name.to_string(),
            member: member_name.clone(),
        });
        crate::e2e_failpoint::maybe_delay("extract.member_start");

        let chunk_dir = Self::member_chunk_dir(output_dir, set_name, &member_name);

        if member.is_symlink || member.is_hardlink || member.is_file_copy {
            if partial_path.exists() || chunk_dir.exists() {
                Self::clear_member_extraction_artifacts(&partial_path, &chunk_dir)?;
            }
            let bytes_written = archive
                .extract_member_to_file(idx, options, None, &out_path)
                .map_err(|error| format!("failed to extract {member_name}: {error}"))?;
            info!(
                job_id = job_id.0,
                set_name,
                member = %member_name,
                bytes_written,
                unpacked_size,
                out_path = %out_path.display(),
                "RAR link member extraction finalized"
            );
            return Ok((member_name, bytes_written, unpacked_size));
        }

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

        let mut partial_file_options = std::fs::OpenOptions::new();
        partial_file_options.create(true).write(true).truncate(true);
        let partial_file = partial_file_options.open(&partial_path).map_err(|e| {
            format!(
                "failed to create partial output {}: {e}",
                partial_path.display()
            )
        })?;
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

        let chunk_records: Result<Vec<(u32, u64)>, weaver_unrar::RarError> = if is_solid {
            let shared_ref = Rc::clone(&shared);
            let checkpoint_ref = Arc::clone(&checkpoint);
            archive
                .extract_member_solid_chunked(idx, options, |absolute_volume| {
                    let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                        weaver_unrar::RarError::CorruptArchive {
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
                    }) as Box<dyn Write>)
                })
                .and_then(|records| {
                    records
                        .into_iter()
                        .map(|(absolute_volume, bytes_written)| {
                            let absolute_volume = u32::try_from(absolute_volume).map_err(|_| {
                                weaver_unrar::RarError::CorruptArchive {
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
            let mut provider_paths = std::collections::HashMap::new();
            for absolute_volume in first_volume..=last_volume {
                let Some(path) = volume_paths.get(&absolute_volume) else {
                    return Err(format!(
                        "missing local RAR volume {absolute_volume} for member {member_name}"
                    ));
                };
                provider_paths.insert((absolute_volume - first_volume) as usize, path.clone());
            }
            let provider = weaver_unrar::StaticVolumeProvider::new(provider_paths);
            let shared_ref = Rc::clone(&shared);
            let checkpoint_ref = Arc::clone(&checkpoint);
            archive
                .extract_member_streaming_chunked(idx, options, &provider, |local_volume| {
                    let volume_index = first_volume + local_volume as u32;
                    Ok(Box::new(DirectOutputWriter {
                        shared: Some(Rc::clone(&shared_ref)),
                        bytes_written: 0,
                        volume_index,
                        checkpoint: Some(Arc::clone(&checkpoint_ref)),
                    }) as Box<dyn Write>)
                })
                .and_then(|records| {
                    records
                        .into_iter()
                        .map(|(local_volume, bytes_written)| {
                            Ok((first_volume + local_volume as u32, bytes_written))
                        })
                        .collect::<Result<Vec<_>, weaver_unrar::RarError>>()
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

        Ok((member_name, bytes_written, unpacked_size))
    }

    pub(crate) fn open_rar_archive_from_snapshot_or_disk(
        set_name: &str,
        volume_paths: std::collections::BTreeMap<u32, PathBuf>,
        password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
        cached_headers: Option<Vec<u8>>,
        shared_kdf_cache: std::sync::Arc<weaver_unrar::crypto::KdfCache>,
        open_mode: RarArchiveOpenMode,
    ) -> Result<crate::pipeline::ArchivePasswordSelection<weaver_unrar::RarArchive>, String> {
        let context = format!("failed to open RAR archive for set '{set_name}'");
        Self::try_rar_password_candidates(&context, &password_candidates, |password| {
            Self::open_rar_archive_from_snapshot_or_disk_with_password(
                set_name,
                &volume_paths,
                cached_headers.as_deref(),
                shared_kdf_cache.clone(),
                open_mode,
                password,
            )
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
        } = request;

        if password_candidates.len() <= 1 {
            let selection = Self::open_rar_archive_from_snapshot_or_disk(
                set_name,
                volume_paths,
                password_candidates,
                cached_headers,
                shared_kdf_cache,
                open_mode,
            )?;
            return Ok(RarExtractionOpenSelection {
                archive: selection.value,
                password: selection.selected_password,
                validated_password: None,
            });
        }

        let context = format!("failed to validate RAR password for set '{set_name}'");
        let selection =
            Self::try_rar_password_candidates(&context, &password_candidates, |password| {
                let mut probe_archive = Self::open_rar_archive_from_snapshot_or_disk_with_password(
                    set_name,
                    &volume_paths,
                    cached_headers.as_deref(),
                    shared_kdf_cache.clone(),
                    open_mode,
                    password,
                )?;
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
                    )?;
                    requires_password
                } else {
                    false
                };
                let archive = Self::open_rar_archive_from_snapshot_or_disk_with_password(
                    set_name,
                    &volume_paths,
                    cached_headers.as_deref(),
                    shared_kdf_cache.clone(),
                    open_mode,
                    password,
                )?;
                Ok((archive, password_validated))
            })?;
        let (archive, password_validated) = selection.value;
        ensure_unique_sanitized_rar_member_paths(&archive)?;
        let password = selection.selected_password;
        let validated_password = password_validated.then(|| password.clone()).flatten();
        Ok(RarExtractionOpenSelection {
            archive,
            password,
            validated_password,
        })
    }

    fn open_rar_archive_from_snapshot_or_disk_with_password(
        set_name: &str,
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
        cached_headers: Option<&[u8]>,
        shared_kdf_cache: std::sync::Arc<weaver_unrar::crypto::KdfCache>,
        open_mode: RarArchiveOpenMode,
        password: Option<&str>,
    ) -> Result<weaver_unrar::RarArchive, crate::pipeline::RarPasswordAttemptError> {
        let has_cached_headers = cached_headers.is_some();
        let refresh_provided_volumes =
            matches!(open_mode, RarArchiveOpenMode::RefreshProvidedVolumes);

        let mut archive = match cached_headers {
            Some(headers) => {
                if let Some(first_path) = volume_paths.get(&0) {
                    let _ = Self::open_rar_volume_zero_with_password(
                        first_path,
                        password,
                        shared_kdf_cache.clone(),
                    )?;
                }
                weaver_unrar::RarArchive::deserialize_headers_with_password_and_shared_kdf_cache(
                    headers,
                    password.map(str::to_string),
                    shared_kdf_cache.clone(),
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
                    shared_kdf_cache.clone(),
                )?
            }
        };

        let retain_attached_readers = !refresh_provided_volumes && archive.is_solid();

        if has_cached_headers && !refresh_provided_volumes && !retain_attached_readers {
            return Ok(archive);
        }

        for (volume_number, path) in volume_paths {
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
                    return Err(crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                        "failed to open RAR volume {volume_number} for set '{set_name}': {error}"
                    )));
                }
            };
            if has_cached_headers
                && refresh_provided_volumes
                && archive.has_volume(*volume_number as usize)
            {
                archive
                    .refresh_volume(*volume_number as usize, Box::new(file))
                    .map_err(|error| {
                        crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                            "failed to refresh RAR volume {volume_number} for set '{set_name}': {error}"
                        ))
                    })?;
            } else if archive.has_volume(*volume_number as usize) {
                archive.attach_volume_reader(*volume_number as usize, Box::new(file));
            } else {
                archive
                    .add_volume(*volume_number as usize, Box::new(file))
                    .map_err(crate::pipeline::RarPasswordAttemptError::from)?;
            }
            if !retain_attached_readers {
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
        shared_kdf_cache: std::sync::Arc<weaver_unrar::crypto::KdfCache>,
    ) -> Result<weaver_unrar::RarArchive, crate::pipeline::RarPasswordAttemptError> {
        let first_file = std::fs::File::open(first_path).map_err(|e| {
            crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                "failed to open RAR volume 0: {e}"
            ))
        })?;
        match password {
            Some(password) => weaver_unrar::RarArchive::open_with_password_and_shared_kdf_cache(
                first_file,
                password,
                shared_kdf_cache,
            ),
            None => {
                weaver_unrar::RarArchive::open_with_shared_kdf_cache(first_file, shared_kdf_cache)
            }
        }
        .map_err(crate::pipeline::RarPasswordAttemptError::from)
    }

    fn select_rar_password_probe_member(
        archive: &weaver_unrar::RarArchive,
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
        archive: &mut weaver_unrar::RarArchive,
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
        idx: usize,
        password: Option<&str>,
    ) -> Result<(), crate::pipeline::RarPasswordAttemptError> {
        let member = archive.member_info(idx).ok_or_else(|| {
            crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                "member index {idx} missing from archive metadata"
            ))
        })?;
        if member.is_directory {
            return Ok(());
        }
        let options = weaver_unrar::ExtractOptions {
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
        let mut provider_paths = std::collections::HashMap::new();
        for absolute_volume in first_volume..=last_volume {
            let Some(path) = volume_paths.get(&absolute_volume) else {
                return Err(crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                    "missing local RAR volume {absolute_volume} for member {}",
                    member.name
                )));
            };
            provider_paths.insert((absolute_volume - first_volume) as usize, path.clone());
        }
        let provider = weaver_unrar::StaticVolumeProvider::new(provider_paths);
        let mut sink = std::io::sink();
        archive
            .extract_member_streaming(idx, &options, &provider, &mut sink)
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
        shared_kdf_cache: std::sync::Arc<weaver_unrar::crypto::KdfCache>,
    ) -> Result<crate::pipeline::ArchivePasswordSelection<weaver_unrar::RarArchive>, String> {
        let selected = candidates.first();
        let password = selected.map(|candidate| candidate.value().to_string());
        weaver_unrar::RarArchive::deserialize_headers_with_password_and_shared_kdf_cache(
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
        shared_kdf_cache: std::sync::Arc<weaver_unrar::crypto::KdfCache>,
    ) -> Result<crate::pipeline::ArchivePasswordSelection<weaver_unrar::RarArchive>, String> {
        let context = format!("failed to parse RAR volume 0 for set '{set_name}'");
        Self::try_rar_password_candidates(&context, candidates, |password| {
            Self::open_rar_volume_zero_with_password(first_path, password, shared_kdf_cache.clone())
        })
    }

    pub(crate) fn rar_error_is_password_related(error: &weaver_unrar::RarError) -> bool {
        matches!(
            error,
            weaver_unrar::RarError::EncryptedArchive
                | weaver_unrar::RarError::EncryptedMember { .. }
                | weaver_unrar::RarError::InvalidPassword
                | weaver_unrar::RarError::WrongPassword { .. }
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
        host_os: weaver_unrar::HostOs,
        attributes: u64,
        is_directory: bool,
        mtime_secs: Option<u64>,
    ) -> weaver_unrar::MemberInfo {
        weaver_unrar::MemberInfo {
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
            compression: weaver_unrar::CompressionInfo {
                format: weaver_unrar::ArchiveFormat::Rar5,
                version: 0,
                method: weaver_unrar::CompressionMethod::Store,
                solid: false,
                dict_size: 0,
            },
            is_encrypted: false,
            hash: None,
            attributes: weaver_unrar::types::FileAttributes(attributes),
            owner: None,
            volumes: weaver_unrar::VolumeSpan {
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
            weaver_unrar::HostOs::Unix,
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

    #[test]
    fn rar_member_mode_translation_matches_unrar_attribute_rules() {
        let readonly_file = metadata_test_member(
            "readonly.txt",
            weaver_unrar::HostOs::Windows,
            0x1,
            false,
            None,
        );
        assert_eq!(
            rar_member_unix_output_mode(&readonly_file),
            Some(0o444 & !current_umask())
        );

        let windows_dir =
            metadata_test_member("dir", weaver_unrar::HostOs::Windows, 0x10, true, None);
        assert_eq!(
            rar_member_unix_output_mode(&windows_dir),
            Some(0o777 & !current_umask())
        );

        let unix_without_mode =
            metadata_test_member("empty-mode", weaver_unrar::HostOs::Unix, 0, false, None);
        assert_eq!(rar_member_unix_output_mode(&unix_without_mode), None);

        let darwin_mode = metadata_test_member(
            "darwin-mode",
            weaver_unrar::HostOs::Darwin,
            0o100640,
            false,
            None,
        );
        assert_eq!(rar_member_unix_output_mode(&darwin_mode), Some(0o640));
    }

    #[test]
    #[cfg(unix)]
    fn rar_server_extraction_delegates_symlink_entries_to_unrar_crate() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../engines/weaver-unrar/tests/fixtures/rar5/rar5_symlink.rar");
        let file = std::fs::File::open(&fixture).unwrap();
        let mut archive = weaver_unrar::RarArchive::open(file).unwrap();
        let symlink_idx = archive
            .metadata()
            .members
            .iter()
            .position(|member| member.is_symlink)
            .expect("fixture should contain a symlink member");
        let member = archive.member_info(symlink_idx).unwrap();
        let member_name = member.name.clone();
        let expected_target = member
            .link_target
            .clone()
            .expect("symlink member should expose target");

        let output_dir = tempfile::tempdir().unwrap();
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(8);
        let volume_paths = std::collections::BTreeMap::new();
        let options = weaver_unrar::ExtractOptions::default();

        let (name, written, total) = Pipeline::extract_rar_member_to_output(
            &mut archive,
            RarExtractionContext {
                volume_paths: &volume_paths,
                event_tx: &event_tx,
                job_id: JobId(42),
                set_name: "rar5-symlink",
                output_dir: output_dir.path(),
                options: &options,
            },
            symlink_idx,
        )
        .unwrap();

        assert_eq!(name, member_name);
        assert_eq!(written, 0);
        assert_eq!(total, 0);

        let out_path = output_dir.path().join(member_name);
        let metadata = std::fs::symlink_metadata(&out_path).unwrap();
        assert!(
            metadata.file_type().is_symlink(),
            "server extraction should create a symlink instead of an empty regular file"
        );
        assert_eq!(
            std::fs::read_link(out_path).unwrap(),
            PathBuf::from(expected_target)
        );
    }
}
