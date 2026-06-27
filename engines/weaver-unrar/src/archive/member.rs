use std::cell::RefCell;
use std::io::{BufReader, BufWriter, Write};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracing::debug;
use zeroize::Zeroize;

use super::*;
use crate::crypto::Blake2spHasher;
use crate::volume::VolumeProvider;

const STREAMING_STORE_CHUNK_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const MAX_LINK_TARGET_BYTES: usize = 0x10000;
const MAX_COMMENT_BYTES: u64 = 0x1000000;

#[cfg(unix)]
fn current_umask() -> u32 {
    static UMASK: OnceLock<u32> = OnceLock::new();
    *UMASK.get_or_init(|| {
        // `umask` is the only portable way to read the current mask. Restore it
        // immediately, matching UnRAR's ConvertAttributes path.
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

#[derive(Clone)]
struct Rar5ReaderCrypto {
    key: [u8; 32],
    hash_key: [u8; 32],
    iv: [u8; 16],
}

impl Drop for Rar5ReaderCrypto {
    fn drop(&mut self) {
        self.key.zeroize();
        self.hash_key.zeroize();
        self.iv.zeroize();
    }
}

#[derive(Clone, Copy)]
enum StoreCopyLimit {
    Exact(u64),
    AtMost(u64),
}

impl StoreCopyLimit {
    fn bytes(self) -> u64 {
        match self {
            Self::Exact(bytes) | Self::AtMost(bytes) => bytes,
        }
    }

    fn is_exact(self) -> bool {
        matches!(self, Self::Exact(_))
    }

    fn is_ceiling(self) -> bool {
        matches!(self, Self::AtMost(_))
    }

    fn next_read_len(self, written: u64, buffer_len: usize) -> usize {
        let remaining = self.bytes().saturating_sub(written);
        buffer_len.min(remaining.min(usize::MAX as u64) as usize)
    }
}

impl RarArchive {
    /// Read the first modern archive comment (`CMT` service), if present.
    pub fn comment(&mut self) -> RarResult<Option<String>> {
        let Some(service) = self
            .services
            .iter()
            .find(|service| service.file_header.name == "CMT")
            .cloned()
        else {
            return Ok(None);
        };

        let payload = self.read_service_subdata_to_memory(&service)?;
        if Self::rar4_comment_is_raw_unicode(&service) {
            return Ok(Some(Self::decode_raw_utf16le_comment(&payload)));
        }
        let end = payload
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(payload.len());
        Ok(Some(String::from_utf8_lossy(&payload[..end]).into_owned()))
    }

    fn rar4_comment_is_raw_unicode(service: &ServiceEntry) -> bool {
        service.file_header.compression.format.is_rar4_family()
            && service.file_header.name == "CMT"
            && (service.file_header.attributes.0 & 0x0001) != 0
    }

    fn decode_raw_utf16le_comment(payload: &[u8]) -> String {
        let mut units = Vec::with_capacity(payload.len() / 2);
        for chunk in payload.chunks_exact(2) {
            let unit = u16::from_le_bytes([chunk[0], chunk[1]]);
            if unit == 0 {
                break;
            }
            units.push(unit);
        }
        String::from_utf16_lossy(&units)
    }

    fn read_service_subdata_to_memory(&mut self, service: &ServiceEntry) -> RarResult<Vec<u8>> {
        let fh = &service.file_header;
        let unpacked_size = fh.unpacked_size.ok_or_else(|| RarError::CorruptArchive {
            detail: format!("RAR service {} has unknown unpacked size", fh.name),
        })?;
        if unpacked_size > MAX_COMMENT_BYTES {
            return Err(RarError::ResourceLimit {
                detail: format!(
                    "RAR service {} is {} bytes, exceeding comment memory limit {}",
                    fh.name, unpacked_size, MAX_COMMENT_BYTES
                ),
            });
        }

        let expected_crc = fh.data_crc32;
        let expected_blake = match service.hash.as_ref() {
            Some(FileHash::Blake2sp(expected)) => Some(*expected),
            _ => None,
        };
        let use_hash_mac = service
            .file_encryption
            .as_ref()
            .is_some_and(|enc| enc.use_hash_mac);
        let rar5_crypto = if service.is_encrypted {
            if fh.compression.format != ArchiveFormat::Rar5 {
                return Err(RarError::EncryptedMember {
                    member: fh.name.clone(),
                });
            }
            let password = self
                .password
                .as_deref()
                .ok_or_else(|| RarError::EncryptedMember {
                    member: fh.name.clone(),
                })?
                .to_owned();
            Some(self.prepare_rar5_encrypted_member(
                &fh.name,
                &password,
                service.file_encryption.as_ref(),
            )?)
        } else {
            None
        };

        let base_reader =
            ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &service.segments, &fh.name);
        let reader: Box<dyn Read + '_> = if let Some(crypto) = rar5_crypto.as_ref() {
            Box::new(crate::crypto::DecryptingReader::new_rar5(
                base_reader,
                &crypto.key,
                &crypto.iv,
            ))
        } else {
            Box::new(base_reader)
        };

        let (output, written, actual_crc, actual_blake) =
            if fh.compression.method == CompressionMethod::Store {
                let mut output = Vec::with_capacity(unpacked_size as usize);
                let mut hash_writer = HashingWriter::new(
                    &mut output,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );
                let written = {
                    Self::copy_reader_to_writer(
                        reader,
                        &mut hash_writer,
                        StoreCopyLimit::Exact(unpacked_size),
                        &fh.name,
                    )?
                };
                hash_writer.flush().map_err(RarError::Io)?;
                let actual_crc = expected_crc.map(|_| hash_writer.finalize_crc());
                let actual_blake = expected_blake.map(|_| hash_writer.finalize_blake2());
                drop(hash_writer);
                (output, written, actual_crc, actual_blake)
            } else {
                let packed_len = service.segments.iter().try_fold(0u64, |total, segment| {
                    total
                        .checked_add(segment.data_size)
                        .ok_or_else(|| RarError::ResourceLimit {
                            detail: format!("RAR service {} packed size overflows u64", fh.name),
                        })
                })?;
                if packed_len > self.limits.max_data_segment {
                    return Err(RarError::ResourceLimit {
                        detail: format!(
                            "RAR service {} packed size {} exceeds limit {}",
                            fh.name, packed_len, self.limits.max_data_segment
                        ),
                    });
                }

                let packed_capacity =
                    usize::try_from(packed_len).map_err(|_| RarError::ResourceLimit {
                        detail: format!(
                            "RAR service {} packed size {} cannot fit in memory on this platform",
                            fh.name, packed_len
                        ),
                    })?;
                let mut packed = Vec::with_capacity(packed_capacity);
                let mut reader = reader;
                reader.read_to_end(&mut packed).map_err(RarError::Io)?;
                if packed.len() as u64 != packed_len {
                    return Err(RarError::CorruptArchive {
                        detail: format!(
                            "RAR service {} read {} packed bytes, expected {}",
                            fh.name,
                            packed.len(),
                            packed_len
                        ),
                    });
                }
                if fh.compression.format == ArchiveFormat::Rar14
                    && fh.name == "CMT"
                    && fh.compression.method != CompressionMethod::Store
                {
                    crate::crypto::decrypt_rar14_packed_comment(&mut packed);
                }

                let output = crate::decompress::decompress_with_limits(
                    &packed,
                    unpacked_size,
                    &fh.compression,
                    None,
                    &self.limits,
                )?;
                let written = output.len() as u64;
                let actual_crc = expected_crc.map(|_| crc32fast::hash(&output));
                let actual_blake = expected_blake.map(|_| {
                    let mut hasher = Blake2spHasher::new();
                    hasher.update(&output);
                    hasher.finalize()
                });
                (output, written, actual_crc, actual_blake)
            };

        if written != unpacked_size {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "RAR service {} produced {} bytes, expected {}",
                    fh.name, written, unpacked_size
                ),
            });
        }
        if let Some(expected) = service.comment_crc16 {
            let actual = (crc32fast::hash(&output) & 0xffff) as u16;
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected: expected as u32,
                    actual: actual as u32,
                });
            }
        }

        Self::verify_member_crc32(
            &fh.name,
            expected_crc,
            actual_crc,
            use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            expected_blake,
            actual_blake,
            use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        Ok(output)
    }

    pub(super) fn hydrate_rar4_uowner_payloads(&mut self) {
        if !self.format.is_rar4_family() {
            return;
        }

        let targets: Vec<(usize, usize)> = self
            .services
            .iter()
            .enumerate()
            .filter(|(_, service)| service.file_header.name == "UOW")
            .filter_map(|(service_idx, service)| {
                let member_idx = self.rar4_service_parent_member_index(service)?;
                self.members[member_idx]
                    .owner
                    .is_none()
                    .then_some((service_idx, member_idx))
            })
            .collect();

        for (service_idx, member_idx) in targets {
            let service = self.services[service_idx].clone();
            match self.read_service_subdata_to_memory(&service) {
                Ok(payload) => {
                    if let Some(owner) = crate::rar4::parse_rar4_uowner_subdata(&payload) {
                        self.members[member_idx].owner = Some(owner);
                    }
                }
                Err(err) => {
                    debug!(
                        service = %service.file_header.name,
                        member = %self.members[member_idx].file_header.name,
                        error = %err,
                        "ignoring unreadable optional RAR4 UOW service payload"
                    );
                }
            }
        }
    }

    fn rar4_service_parent_member_index(&self, service: &ServiceEntry) -> Option<usize> {
        let service_segment = service.segments.first()?;
        self.members
            .iter()
            .enumerate()
            .filter_map(|(idx, member)| {
                let key = member
                    .segments
                    .iter()
                    .filter_map(|segment| {
                        (segment.volume_index < service_segment.volume_index
                            || (segment.volume_index == service_segment.volume_index
                                && segment.data_offset < service_segment.data_offset))
                            .then_some((segment.volume_index, segment.data_offset))
                    })
                    .max()?;
                Some((idx, key))
            })
            .max_by_key(|(_, key)| *key)
            .map(|(idx, _)| idx)
    }

    fn link_target_from_rar3_payload(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        fh: &FileHeader,
    ) -> RarResult<String> {
        let link_options = ExtractOptions {
            verify: false,
            password: options.password.clone(),
            restore_owners: false,
        };
        let payload = self
            .extract_member(index, &link_options, None)?
            .into_bytes()?;
        if payload.len() > MAX_LINK_TARGET_BYTES {
            return Err(RarError::ResourceLimit {
                detail: format!(
                    "RAR3 Unix symlink target for {} is {} bytes, exceeding MAXPATHSIZE {}",
                    fh.name,
                    payload.len(),
                    MAX_LINK_TARGET_BYTES
                ),
            });
        }

        let nul_pos = payload
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(payload.len());
        let target_bytes = &payload[..nul_pos];
        if options.verify
            && let Some(expected) = fh.data_crc32
        {
            let actual = crc32fast::hash(target_bytes);
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        Ok(String::from_utf8_lossy(target_bytes).into_owned())
    }

    fn remove_existing_link_output(out_path: &std::path::Path) -> RarResult<()> {
        match std::fs::symlink_metadata(out_path) {
            Ok(metadata) => {
                if metadata.file_type().is_dir() {
                    return Err(RarError::Io(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "link output path {} is an existing directory",
                            out_path.display()
                        ),
                    )));
                }
                std::fs::remove_file(out_path).map_err(RarError::Io)?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(RarError::Io(err)),
        }
        Ok(())
    }

    fn remove_existing_regular_output_symlink(out_path: &std::path::Path) -> RarResult<()> {
        match std::fs::symlink_metadata(out_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                std::fs::remove_file(out_path).map_err(RarError::Io)?;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(RarError::Io(err)),
        }
        Ok(())
    }

    fn ensure_output_parent_dir(out_path: &std::path::Path) -> RarResult<()> {
        if let Some(parent) = out_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(RarError::Io)?;
        }
        Ok(())
    }

    fn ensure_output_parent_dir_for_member(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        let member_path = std::path::Path::new(member_name);
        if member_name.is_empty() || !out_path.ends_with(member_path) {
            return Self::ensure_output_parent_dir(out_path);
        }

        let root = Self::redirection_target_root(member_name, out_path);
        if !root.as_os_str().is_empty() {
            std::fs::create_dir_all(&root).map_err(RarError::Io)?;
        }

        let Some(member_parent) = member_path.parent() else {
            return Ok(());
        };
        let mut current = root;
        for component in member_parent.components() {
            let std::path::Component::Normal(part) = component else {
                continue;
            };
            current.push(part);
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    std::fs::remove_file(&current).map_err(RarError::Io)?;
                    std::fs::create_dir(&current).map_err(RarError::Io)?;
                }
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(RarError::Io(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "output parent path {} exists and is not a directory",
                            current.display()
                        ),
                    )));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(&current).map_err(RarError::Io)?;
                }
                Err(err) => return Err(RarError::Io(err)),
            }
        }

        Ok(())
    }

    fn ensure_safe_link_target(
        member_name: &str,
        target: &str,
        raw_member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        Self::ensure_safe_symlink_target(member_name, target, raw_member_name, false, out_path)
    }

    fn ensure_safe_symlink_target(
        member_name: &str,
        target: &str,
        raw_member_name: &str,
        allow_windows_drive_relative: bool,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        if !crate::path::is_safe_symlink_target(member_name, target, allow_windows_drive_relative) {
            return Err(RarError::UnsafeLinkTarget {
                member: raw_member_name.to_string(),
                target: target.to_string(),
            });
        }
        if Self::link_target_has_parent_traversal(target)
            && Self::source_path_has_existing_link_or_non_dir(member_name, out_path)?
        {
            return Err(RarError::UnsafeLinkTarget {
                member: raw_member_name.to_string(),
                target: target.to_string(),
            });
        }
        Ok(())
    }

    fn link_target_has_parent_traversal(target: &str) -> bool {
        target
            .replace('\\', "/")
            .split('/')
            .any(|part| part == "..")
    }

    fn source_path_has_existing_link_or_non_dir(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<bool> {
        let parent_depth = member_name
            .split('/')
            .filter(|part| !part.is_empty())
            .count()
            .saturating_sub(1);
        let mut current = out_path.parent();

        for _ in 0..parent_depth {
            let Some(path) = current else {
                break;
            };
            match std::fs::symlink_metadata(path) {
                Ok(metadata) => {
                    let file_type = metadata.file_type();
                    if file_type.is_symlink() || !file_type.is_dir() {
                        return Ok(true);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(RarError::Io(err)),
            }
            current = path.parent();
        }

        Ok(false)
    }

    fn normalize_windows_redirection_target(target: &str) -> String {
        let target = target
            .strip_prefix("\\??\\")
            .or_else(|| target.strip_prefix("/??/"))
            .unwrap_or(target);
        target.replace('\\', "/")
    }

    fn normalized_rar5_redirection_target(
        redir_type: header::RedirectionType,
        target: &str,
    ) -> String {
        match redir_type {
            header::RedirectionType::UnixSymlink => target.to_string(),
            header::RedirectionType::WindowsSymlink
            | header::RedirectionType::WindowsJunction
            | header::RedirectionType::Hardlink
            | header::RedirectionType::FileCopy => {
                Self::normalize_windows_redirection_target(target)
            }
            header::RedirectionType::Unknown(_) => target.to_string(),
        }
    }

    fn redirection_target_root(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> std::path::PathBuf {
        let member_path = std::path::Path::new(member_name);
        if !member_name.is_empty() && out_path.ends_with(member_path) {
            let mut root = out_path;
            for _ in member_path.components() {
                root = root.parent().unwrap_or_else(|| std::path::Path::new(""));
            }
            root.to_path_buf()
        } else {
            out_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new(""))
                .to_path_buf()
        }
    }

    fn redirection_target_path(
        member_name: &str,
        target: &str,
        out_path: &std::path::Path,
    ) -> std::path::PathBuf {
        Self::redirection_target_root(member_name, out_path)
            .join(crate::path::sanitize_path(target))
    }

    fn create_symlink_output(
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        options: &ExtractOptions,
        member_name: &str,
        raw_member_name: &str,
        target: &str,
        allow_windows_drive_relative: bool,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        Self::ensure_safe_symlink_target(
            member_name,
            target,
            raw_member_name,
            allow_windows_drive_relative,
            out_path,
        )?;
        Self::remove_existing_link_output(out_path)?;

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, out_path).map_err(RarError::Io)?;
            Self::apply_symlink_times(fh, out_path)?;
            if options.restore_owners {
                Self::apply_unix_owner(owner, out_path, false)?;
            }
            Ok(0)
        }

        #[cfg(not(unix))]
        {
            let _ = fh;
            let _ = owner;
            let _ = options;
            let _ = out_path;
            Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "symlink".to_string(),
            })
        }
    }

    fn apply_symlink_times(fh: &FileHeader, out_path: &std::path::Path) -> RarResult<()> {
        let has_mtime = fh.mtime.is_some();
        let has_atime = fh.atime.is_some();
        if !has_mtime && !has_atime {
            return Ok(());
        }

        #[cfg(unix)]
        {
            let mtime = fh
                .mtime
                .map(filetime::FileTime::from_system_time)
                .unwrap_or_else(filetime::FileTime::now);
            let atime = fh
                .atime
                .map(filetime::FileTime::from_system_time)
                .unwrap_or_else(filetime::FileTime::now);
            filetime::set_symlink_file_times(out_path, atime, mtime).map_err(RarError::Io)
        }

        #[cfg(not(unix))]
        {
            let _ = fh;
            let _ = out_path;
            Ok(())
        }
    }

    fn apply_output_times(fh: &FileHeader, out_path: &std::path::Path) -> RarResult<()> {
        match (fh.mtime, fh.atime) {
            (Some(mtime), Some(atime)) => filetime::set_file_times(
                out_path,
                filetime::FileTime::from_system_time(atime),
                filetime::FileTime::from_system_time(mtime),
            )
            .map_err(RarError::Io),
            (Some(mtime), None) => {
                filetime::set_file_mtime(out_path, filetime::FileTime::from_system_time(mtime))
                    .map_err(RarError::Io)
            }
            (None, Some(atime)) => {
                filetime::set_file_atime(out_path, filetime::FileTime::from_system_time(atime))
                    .map_err(RarError::Io)
            }
            (None, None) => Ok(()),
        }
    }

    fn apply_unix_permissions(fh: &FileHeader, out_path: &std::path::Path) -> RarResult<()> {
        #[cfg(unix)]
        {
            if let Some(mode) = Self::unix_output_mode(fh) {
                use std::os::unix::fs::PermissionsExt;

                let mut permissions = std::fs::metadata(out_path)
                    .map_err(RarError::Io)?
                    .permissions();
                permissions.set_mode(mode);
                std::fs::set_permissions(out_path, permissions).map_err(RarError::Io)?;
            }
        }

        #[cfg(not(unix))]
        {
            let _ = fh;
            let _ = out_path;
        }

        Ok(())
    }

    fn apply_unix_owner(
        owner: Option<&crate::types::UnixOwnerInfo>,
        out_path: &std::path::Path,
        follow_links: bool,
    ) -> RarResult<()> {
        #[cfg(unix)]
        {
            let Some(owner) = owner else {
                return Ok(());
            };
            let Some((uid, gid)) = Self::resolve_unix_owner(owner)? else {
                return Ok(());
            };
            if Self::path_owner_matches(out_path, uid, gid, follow_links)? {
                return Ok(());
            }

            use std::os::unix::ffi::OsStrExt;

            let path = std::ffi::CString::new(out_path.as_os_str().as_bytes()).map_err(|_| {
                RarError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("output path {} contains NUL byte", out_path.display()),
                ))
            })?;
            let uid_arg = uid.unwrap_or(!0 as libc::uid_t);
            let gid_arg = gid.unwrap_or(!0 as libc::gid_t);
            let result = unsafe {
                if follow_links {
                    libc::chown(path.as_ptr(), uid_arg, gid_arg)
                } else {
                    libc::lchown(path.as_ptr(), uid_arg, gid_arg)
                }
            };
            if result != 0 {
                return Err(RarError::Io(std::io::Error::last_os_error()));
            }
        }

        #[cfg(not(unix))]
        {
            let _ = owner;
            let _ = out_path;
            let _ = follow_links;
        }

        Ok(())
    }

    #[cfg(unix)]
    fn resolve_unix_owner(
        owner: &crate::types::UnixOwnerInfo,
    ) -> RarResult<Option<(Option<libc::uid_t>, Option<libc::gid_t>)>> {
        let mut uid = owner.uid.map(Self::uid_from_archive).transpose()?;
        let mut gid = owner.gid.map(Self::gid_from_archive).transpose()?;

        if let Some(name) = owner.user_name.as_deref().filter(|name| !name.is_empty()) {
            match Self::lookup_uid_by_name(name)? {
                Some(resolved) => uid = Some(resolved),
                None if uid.is_none() => return Ok(None),
                None => {}
            }
        }

        if let Some(name) = owner.group_name.as_deref().filter(|name| !name.is_empty()) {
            match Self::lookup_gid_by_name(name)? {
                Some(resolved) => gid = Some(resolved),
                None if gid.is_none() => return Ok(None),
                None => {}
            }
        }

        if uid.is_none() && gid.is_none() {
            Ok(None)
        } else {
            Ok(Some((uid, gid)))
        }
    }

    #[cfg(unix)]
    fn uid_from_archive(uid: u64) -> RarResult<libc::uid_t> {
        libc::uid_t::try_from(uid).map_err(|_| RarError::CorruptArchive {
            detail: format!("Unix owner id {uid} exceeds platform uid_t"),
        })
    }

    #[cfg(unix)]
    fn gid_from_archive(gid: u64) -> RarResult<libc::gid_t> {
        libc::gid_t::try_from(gid).map_err(|_| RarError::CorruptArchive {
            detail: format!("Unix group id {gid} exceeds platform gid_t"),
        })
    }

    #[cfg(unix)]
    fn lookup_uid_by_name(name: &str) -> RarResult<Option<libc::uid_t>> {
        let name = match std::ffi::CString::new(name) {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        let passwd = unsafe { libc::getpwnam(name.as_ptr()) };
        if passwd.is_null() {
            Ok(None)
        } else {
            Ok(Some(unsafe { (*passwd).pw_uid }))
        }
    }

    #[cfg(unix)]
    fn lookup_gid_by_name(name: &str) -> RarResult<Option<libc::gid_t>> {
        let name = match std::ffi::CString::new(name) {
            Ok(name) => name,
            Err(_) => return Ok(None),
        };
        let group = unsafe { libc::getgrnam(name.as_ptr()) };
        if group.is_null() {
            Ok(None)
        } else {
            Ok(Some(unsafe { (*group).gr_gid }))
        }
    }

    #[cfg(unix)]
    fn path_owner_matches(
        out_path: &std::path::Path,
        uid: Option<libc::uid_t>,
        gid: Option<libc::gid_t>,
        follow_links: bool,
    ) -> RarResult<bool> {
        use std::os::unix::fs::MetadataExt;

        let metadata = if follow_links {
            std::fs::metadata(out_path).map_err(RarError::Io)?
        } else {
            std::fs::symlink_metadata(out_path).map_err(RarError::Io)?
        };
        let uid_matches = uid.is_none_or(|uid| u64::from(metadata.uid()) == uid as u64);
        let gid_matches = gid.is_none_or(|gid| u64::from(metadata.gid()) == gid as u64);
        Ok(uid_matches && gid_matches)
    }

    fn unix_output_mode(fh: &FileHeader) -> Option<u32> {
        match fh.host_os {
            crate::types::HostOs::Unix => {
                let mode = fh.attributes.unix_mode() & 0o7777;
                (mode != 0).then_some(mode)
            }
            crate::types::HostOs::Windows => {
                let mode = if fh.is_directory || fh.attributes.is_directory_attr() {
                    0o777
                } else if fh.attributes.is_readonly() {
                    0o444
                } else {
                    0o666
                };
                Some(mode & !current_umask())
            }
            crate::types::HostOs::Unknown(_) => {
                let mode = if fh.is_directory { 0o777 } else { 0o666 };
                Some(mode & !current_umask())
            }
        }
    }

    fn apply_output_metadata(
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        options: &ExtractOptions,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        if options.restore_owners {
            Self::apply_unix_owner(owner, out_path, true)?;
        }
        Self::apply_output_times(fh, out_path)?;
        Self::apply_unix_permissions(fh, out_path)
    }

    fn create_hardlink_output(
        fh: &FileHeader,
        member_name: &str,
        raw_member_name: &str,
        target: &str,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        Self::ensure_safe_link_target(member_name, target, raw_member_name, out_path)?;
        Self::remove_existing_link_output(out_path)?;
        let target_path = Self::redirection_target_path(member_name, target, out_path);
        std::fs::hard_link(&target_path, out_path).map_err(RarError::Io)?;
        Self::apply_unix_permissions(fh, out_path)?;
        Ok(0)
    }

    fn copy_existing_filecopy_source(
        source_path: &std::path::Path,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        let metadata = std::fs::metadata(source_path).map_err(RarError::Io)?;
        if !metadata.is_file() {
            return Err(RarError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "RAR file-copy source {} is not a regular file",
                    source_path.display()
                ),
            )));
        }
        std::fs::copy(source_path, out_path).map_err(RarError::Io)
    }

    fn unique_filecopy_temp_path(out_path: &std::path::Path, index: usize) -> std::path::PathBuf {
        let base = out_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new(""));
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        base.join(format!(
            ".weaver-unrar-filecopy-{}-{index}-{stamp}.tmp",
            std::process::id()
        ))
    }

    fn create_filecopy_output(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        member_name: &str,
        raw_member_name: &str,
        target: &str,
        target_is_directory: bool,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        if target_is_directory {
            return Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "file-copy directory target".to_string(),
            });
        }

        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        Self::ensure_safe_link_target(member_name, target, raw_member_name, out_path)?;
        let target_path = Self::redirection_target_path(member_name, target, out_path);
        if target_path == out_path {
            return Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "self-referential file copy".to_string(),
            });
        }

        if target_path.exists() {
            Self::remove_existing_link_output(out_path)?;
            let written = Self::copy_existing_filecopy_source(&target_path, out_path)?;
            Self::apply_output_metadata(fh, owner, options, out_path)?;
            return Ok(written);
        }

        let target_member_name = crate::path::sanitize_path(target);
        if target_member_name == member_name {
            return Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "self-referential file copy".to_string(),
            });
        }

        let source_index = self
            .members
            .iter()
            .position(|entry| {
                crate::path::sanitize_path(&entry.file_header.name) == target_member_name
            })
            .ok_or_else(|| {
                RarError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("RAR file-copy source member {target} was not found"),
                ))
            })?;

        if source_index == index {
            return Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "self-referential file copy".to_string(),
            });
        }

        if self.members[source_index].file_header.is_directory {
            return Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "file-copy source directory".to_string(),
            });
        }

        if let Some(redirection) = &self.members[source_index].redirection
            && !matches!(redirection.redir_type, header::RedirectionType::Hardlink)
        {
            return Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "file-copy source redirection".to_string(),
            });
        }

        let temp_path = Self::unique_filecopy_temp_path(out_path, index);
        let extract_result = self.extract_member_to_file(source_index, options, None, &temp_path);
        if let Err(error) = extract_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }

        Self::remove_existing_link_output(out_path)?;
        let copy_result = Self::copy_existing_filecopy_source(&temp_path, out_path);
        let remove_result = std::fs::remove_file(&temp_path);
        match (copy_result, remove_result) {
            (Ok(written), Ok(())) => {
                Self::apply_output_metadata(fh, owner, options, out_path)?;
                Ok(written)
            }
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(RarError::Io(error)),
        }
    }

    fn extract_link_member_to_file(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        redirection: Option<header::Redirection>,
        out_path: &std::path::Path,
    ) -> RarResult<Option<u64>> {
        let member_name = crate::path::sanitize_path(&fh.name);

        if let Some(redir) = redirection {
            let link_type = format!("{:?}", redir.redir_type);
            let target = Self::normalized_rar5_redirection_target(redir.redir_type, &redir.target);
            return match redir.redir_type {
                header::RedirectionType::UnixSymlink
                | header::RedirectionType::WindowsSymlink
                | header::RedirectionType::WindowsJunction => Self::create_symlink_output(
                    fh,
                    owner,
                    options,
                    &member_name,
                    &fh.name,
                    &target,
                    matches!(
                        redir.redir_type,
                        header::RedirectionType::WindowsSymlink
                            | header::RedirectionType::WindowsJunction
                    ),
                    out_path,
                )
                .map(Some),
                header::RedirectionType::Hardlink => {
                    Self::create_hardlink_output(fh, &member_name, &fh.name, &target, out_path)
                        .map(Some)
                }
                header::RedirectionType::FileCopy => self
                    .create_filecopy_output(
                        index,
                        options,
                        fh,
                        owner,
                        &member_name,
                        &fh.name,
                        &target,
                        redir.target_is_directory,
                        out_path,
                    )
                    .map(Some),
                header::RedirectionType::Unknown(_) => Err(RarError::UnsupportedLinkType {
                    member: fh.name.clone(),
                    link_type,
                }),
            };
        }

        let rar3_unix_symlink = self.format.is_rar4_family()
            && matches!(fh.host_os, crate::types::HostOs::Unix)
            && (fh.attributes.0 & 0xF000) == 0xA000;
        if rar3_unix_symlink {
            let target = self.link_target_from_rar3_payload(index, options, fh)?;
            return Self::create_symlink_output(
                fh,
                owner,
                options,
                &member_name,
                &fh.name,
                &target,
                false,
                out_path,
            )
            .map(Some);
        }

        Ok(None)
    }

    fn advance_solid_cursor_to(&mut self, index: usize, fh: &FileHeader) -> RarResult<()> {
        if index < self.solid_next_index {
            return Err(RarError::SolidOrderViolation {
                required: format!("member index {}", self.solid_next_index),
                requested: fh.name.clone(),
            });
        }

        while self.solid_next_index < index {
            let skip_idx = self.solid_next_index;
            let skip_entry = self.members[skip_idx].clone();
            let skip_fh = skip_entry.file_header;
            self.enforce_archive_member_limits(&skip_fh)?;

            if skip_fh.compression.method != CompressionMethod::Store {
                let skip_unpacked = self.target_unpacked_size(&skip_fh);
                let rar5_crypto = if skip_entry.is_encrypted && self.format == ArchiveFormat::Rar5 {
                    let password =
                        self.password
                            .as_deref()
                            .ok_or_else(|| RarError::EncryptedMember {
                                member: skip_fh.name.clone(),
                            })?;
                    Some(self.prepare_rar5_encrypted_member(
                        &skip_fh.name,
                        password,
                        skip_entry.file_encryption.as_ref(),
                    )?)
                } else {
                    None
                };
                let base_reader = ArchiveSegmentReader::new(
                    &mut self.volumes,
                    &self.limits,
                    &skip_entry.segments,
                    &skip_fh.name,
                );

                if skip_entry.is_encrypted {
                    let password = self
                        .password
                        .as_deref()
                        .ok_or_else(|| RarError::EncryptedMember {
                            member: skip_fh.name.clone(),
                        })?
                        .to_owned();

                    if self.format.is_rar4_family() {
                        let reader = Self::wrap_rar4_encrypted_reader(
                            &self.kdf_cache,
                            base_reader,
                            &skip_fh,
                            &password,
                            skip_entry.rar4_salt,
                        )?;
                        let written = Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                        self.enforce_unknown_lz_output_limit(&skip_fh, written)?;
                    } else {
                        let crypto =
                            rar5_crypto
                                .as_ref()
                                .ok_or_else(|| RarError::CorruptArchive {
                                    detail: format!(
                                        "member {} is missing RAR5 crypto material",
                                        skip_fh.name
                                    ),
                                })?;
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &crypto.key,
                            &crypto.iv,
                        );
                        let written = Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                        self.enforce_unknown_lz_output_limit(&skip_fh, written)?;
                    }
                } else {
                    let written = Self::solid_decode_reader_to_sink(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        skip_unpacked,
                        &skip_fh,
                    )?;
                    self.enforce_unknown_lz_output_limit(&skip_fh, written)?;
                }
            }

            self.solid_next_index += 1;
        }

        Ok(())
    }

    fn normalized_provider_segments(segments: &[DataSegment]) -> (Vec<DataSegment>, usize) {
        let mut sorted_segments = segments.to_vec();
        sorted_segments.sort_by_key(|segment| segment.volume_index);
        let volume_base = sorted_segments
            .first()
            .map_or(0, |segment| segment.volume_index);
        for segment in &mut sorted_segments {
            segment.volume_index -= volume_base;
        }
        (sorted_segments, volume_base)
    }

    fn advance_solid_cursor_to_streaming(
        &mut self,
        index: usize,
        fh: &FileHeader,
        provider: &dyn VolumeProvider,
    ) -> RarResult<()> {
        if index < self.solid_next_index {
            return Err(RarError::SolidOrderViolation {
                required: format!("member index {}", self.solid_next_index),
                requested: fh.name.clone(),
            });
        }

        while self.solid_next_index < index {
            let skip_idx = self.solid_next_index;
            let skip_entry = self.members[skip_idx].clone();
            let skip_fh = skip_entry.file_header;
            self.enforce_archive_member_limits(&skip_fh)?;

            if skip_fh.compression.method != CompressionMethod::Store {
                let skip_unpacked = self.target_unpacked_size(&skip_fh);
                let (segments, _) = Self::normalized_provider_segments(&skip_entry.segments);
                let base_reader = ChainedSegmentReader::new(&segments, provider)
                    .with_max_data_segment(self.limits.max_data_segment)
                    .with_continuation(skip_fh.split_after, self.format, self.password.clone());

                if skip_entry.is_encrypted {
                    let password = self
                        .password
                        .as_deref()
                        .ok_or_else(|| RarError::EncryptedMember {
                            member: skip_fh.name.clone(),
                        })?
                        .to_owned();

                    if self.format.is_rar4_family() {
                        let reader = Self::wrap_rar4_encrypted_reader(
                            &self.kdf_cache,
                            base_reader,
                            &skip_fh,
                            &password,
                            skip_entry.rar4_salt,
                        )?;
                        let written = Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                        self.enforce_unknown_lz_output_limit(&skip_fh, written)?;
                    } else {
                        let crypto = self.prepare_rar5_encrypted_member(
                            &skip_fh.name,
                            &password,
                            skip_entry.file_encryption.as_ref(),
                        )?;
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &crypto.key,
                            &crypto.iv,
                        );
                        let written = Self::solid_decode_reader_to_sink(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            skip_unpacked,
                            &skip_fh,
                        )?;
                        self.enforce_unknown_lz_output_limit(&skip_fh, written)?;
                    }
                } else {
                    let written = Self::solid_decode_reader_to_sink(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        skip_unpacked,
                        &skip_fh,
                    )?;
                    self.enforce_unknown_lz_output_limit(&skip_fh, written)?;
                }
            }

            self.solid_next_index += 1;
        }

        Ok(())
    }

    fn enforce_archive_member_limits(&self, fh: &FileHeader) -> RarResult<()> {
        if fh.data_size > self.limits.max_data_segment {
            return Err(RarError::ResourceLimit {
                detail: format!(
                    "member {} compressed data size {} exceeds maximum {}",
                    fh.name, fh.data_size, self.limits.max_data_segment
                ),
            });
        }

        if let Some(unpacked_size) = fh.unpacked_size
            && unpacked_size > self.limits.max_unpacked_size
        {
            return Err(RarError::ResourceLimit {
                detail: format!(
                    "member {} unpacked size {} exceeds maximum {}",
                    fh.name, unpacked_size, self.limits.max_unpacked_size
                ),
            });
        }

        let dict_size = Self::effective_member_dict_size(fh);
        if dict_size > self.limits.max_dict_size {
            return Err(RarError::DictionaryTooLarge {
                size: dict_size,
                max: self.limits.max_dict_size,
            });
        }

        Ok(())
    }

    fn effective_member_dict_size(fh: &FileHeader) -> u64 {
        if fh.compression.method == CompressionMethod::Store {
            0
        } else if fh.compression.format.is_rar4_family() {
            crate::decompress::rar4_old::effective_rar4_window_size(fh.compression.dict_size)
        } else {
            crate::decompress::lz::effective_lz_window_size(fh.compression.dict_size)
        }
    }

    fn target_unpacked_size(&self, fh: &FileHeader) -> u64 {
        fh.unpacked_size.unwrap_or(self.limits.max_unpacked_size)
    }

    fn output_capacity_hint(&self, fh: &FileHeader) -> usize {
        fh.unpacked_size
            .unwrap_or(0)
            .min(self.limits.max_unpacked_size)
            .min(usize::MAX as u64) as usize
    }

    fn store_copy_limit(&self, fh: &FileHeader) -> StoreCopyLimit {
        fh.unpacked_size
            .map(StoreCopyLimit::Exact)
            .unwrap_or(StoreCopyLimit::AtMost(self.limits.max_unpacked_size))
    }

    fn resource_limit_error(member_name: &str, written: u64, limit: u64) -> RarError {
        RarError::ResourceLimit {
            detail: format!(
                "member {member_name} output size exceeded maximum {limit} bytes after writing {written} bytes"
            ),
        }
    }

    fn enforce_store_ceiling_not_exceeded<R: Read>(
        reader: &mut R,
        limit: StoreCopyLimit,
        member_name: &str,
    ) -> RarResult<()> {
        if !limit.is_ceiling() {
            return Ok(());
        }

        let mut probe = [0u8; 1];
        let read = reader.read(&mut probe).map_err(RarError::Io)?;
        if read == 0 {
            return Ok(());
        }

        Err(Self::resource_limit_error(
            member_name,
            limit.bytes() + read as u64,
            limit.bytes(),
        ))
    }

    fn enforce_unknown_lz_output_limit(&self, fh: &FileHeader, written: u64) -> RarResult<()> {
        if fh.unpacked_size.is_none() && written >= self.limits.max_unpacked_size {
            return Err(Self::resource_limit_error(
                &fh.name,
                written,
                self.limits.max_unpacked_size,
            ));
        }

        Ok(())
    }

    fn enforce_unknown_lz_chunk_limit(
        &self,
        fh: &FileHeader,
        chunks: &[(usize, u64)],
    ) -> RarResult<()> {
        let written = chunks
            .iter()
            .fold(0u64, |sum, (_, bytes)| sum.saturating_add(*bytes));
        self.enforce_unknown_lz_output_limit(fh, written)
    }

    fn wrap_rar4_encrypted_reader<R: Read>(
        kdf_cache: &crate::crypto::KdfCache,
        reader: R,
        fh: &FileHeader,
        password: &str,
        rar4_salt: Option<[u8; 8]>,
    ) -> RarResult<crate::crypto::DecryptingReader<R>> {
        let method = if fh.compression.format == ArchiveFormat::Rar14 {
            crate::rar4::types::Rar4EncryptionMethod::Rar13
        } else {
            crate::rar4::types::Rar4EncryptionMethod::for_unpack_version(fh.compression.version)
        };
        let decrypting_reader = match method {
            crate::rar4::types::Rar4EncryptionMethod::Rar30 => {
                let (mut key, mut iv) = kdf_cache.derive_key_rar4(password, rar4_salt.as_ref());
                let decrypting_reader =
                    crate::crypto::DecryptingReader::new_rar4(reader, &key, &iv);
                key.zeroize();
                iv.zeroize();
                decrypting_reader
            }
            legacy => crate::crypto::DecryptingReader::new_rar4_legacy(reader, legacy, password),
        };
        Ok(decrypting_reader)
    }

    fn prepare_rar5_encrypted_member(
        &self,
        member_name: &str,
        password: &str,
        file_encryption: Option<&FileEncryptionInfo>,
    ) -> RarResult<Rar5ReaderCrypto> {
        let enc_info = file_encryption.ok_or_else(|| RarError::CorruptArchive {
            detail: format!(
                "member {} is marked encrypted but has no encryption parameters",
                member_name,
            ),
        })?;

        if enc_info.version > 0 {
            return Err(RarError::UnsupportedEncryption {
                version: enc_info.version,
            });
        }
        if enc_info.kdf_count > crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX {
            return Err(RarError::UnsupportedEncryptionKdf {
                count: enc_info.kdf_count,
                max: crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX,
            });
        }

        if let Some(check_data) = enc_info.check_data
            && !self.kdf_cache.verify_password_rar5(
                password,
                &enc_info.salt,
                enc_info.kdf_count,
                &check_data,
            )
        {
            return Err(RarError::WrongPassword {
                member: member_name.to_string(),
            });
        }

        let mut material =
            self.kdf_cache
                .derive_material_rar5(password, &enc_info.salt, enc_info.kdf_count)?;

        let crypto = Rar5ReaderCrypto {
            key: material.key,
            hash_key: material.hash_key,
            iv: enc_info.iv,
        };
        material.key.zeroize();
        material.hash_key.zeroize();
        material.psw_check.zeroize();

        Ok(crypto)
    }

    fn verify_member_crc32(
        member_name: &str,
        expected: Option<u32>,
        actual: Option<u32>,
        use_hash_mac: bool,
        hash_key: Option<&[u8; 32]>,
    ) -> RarResult<()> {
        let (Some(expected), Some(actual)) = (expected, actual) else {
            return Ok(());
        };

        let actual = if use_hash_mac {
            crate::crypto::convert_crc32_to_mac(
                actual,
                hash_key.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!("member {member_name} is missing a RAR5 hash key"),
                })?,
            )
        } else {
            actual
        };

        if actual != expected {
            return Err(RarError::DataCrcMismatch {
                member: member_name.to_string(),
                expected,
                actual,
            });
        }

        Ok(())
    }

    fn verify_member_data_hash(
        member_name: &str,
        expected: Option<crate::types::DataHash>,
        actual: Option<crate::types::DataHash>,
        use_hash_mac: bool,
        hash_key: Option<&[u8; 32]>,
    ) -> RarResult<()> {
        let (Some(expected), Some(actual)) = (expected, actual) else {
            return Ok(());
        };

        match (expected, actual) {
            (crate::types::DataHash::Crc32(expected), crate::types::DataHash::Crc32(actual)) => {
                Self::verify_member_crc32(
                    member_name,
                    Some(expected),
                    Some(actual),
                    use_hash_mac,
                    hash_key,
                )
            }
            (crate::types::DataHash::Rar14(expected), crate::types::DataHash::Rar14(actual)) => {
                if actual != expected {
                    return Err(RarError::DataCrcMismatch {
                        member: member_name.to_string(),
                        expected: u32::from(expected),
                        actual: u32::from(actual),
                    });
                }
                Ok(())
            }
            _ => Err(RarError::CorruptArchive {
                detail: format!("member {member_name} used mismatched data hash types"),
            }),
        }
    }

    fn expected_rar14_hash(fh: &FileHeader, verify: bool) -> Option<crate::types::DataHash> {
        if !verify {
            return None;
        }
        match fh.data_hash {
            Some(crate::types::DataHash::Rar14(_)) => fh.data_hash,
            _ => None,
        }
    }

    fn verify_member_blake2(
        member_name: &str,
        expected: Option<[u8; 32]>,
        actual: Option<[u8; 32]>,
        use_hash_mac: bool,
        hash_key: Option<&[u8; 32]>,
    ) -> RarResult<()> {
        let (Some(expected), Some(actual)) = (expected, actual) else {
            return Ok(());
        };

        let actual = if use_hash_mac {
            crate::crypto::convert_blake2_to_mac(
                actual,
                hash_key.ok_or_else(|| RarError::CorruptArchive {
                    detail: format!("member {member_name} is missing a RAR5 hash key"),
                })?,
            )
        } else {
            actual
        };

        if actual != expected {
            return Err(RarError::Blake2Mismatch {
                member: member_name.to_string(),
            });
        }

        Ok(())
    }

    fn copy_reader_to_writer<R: Read, W: Write>(
        mut reader: R,
        writer: &mut W,
        limit: StoreCopyLimit,
        member_name: &str,
    ) -> RarResult<u64> {
        let mut buffer = vec![0u8; STREAMING_STORE_CHUNK_BUFFER_BYTES];
        let mut written = 0u64;

        loop {
            let to_read = limit.next_read_len(written, buffer.len());
            if to_read == 0 {
                Self::enforce_store_ceiling_not_exceeded(&mut reader, limit, member_name)?;
                break;
            }

            let read = reader.read(&mut buffer[..to_read]).map_err(RarError::Io)?;
            if read == 0 {
                if limit.is_exact() {
                    return Err(RarError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while copying member data",
                    )));
                }
                break;
            }
            writer.write_all(&buffer[..read]).map_err(RarError::Io)?;
            written += read as u64;
        }

        Ok(written)
    }

    fn copy_reader_to_writer_chunked<R: Read, F>(
        mut reader: R,
        volume_tracker: Arc<AtomicUsize>,
        first_volume_index: usize,
        mut writer_factory: F,
        limit: StoreCopyLimit,
        member_name: &str,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let mut buffer = vec![0u8; STREAMING_STORE_CHUNK_BUFFER_BYTES];
        let mut current_volume = first_volume_index;
        let mut current_writer = Some(writer_factory(current_volume)?);
        let mut current_written = 0u64;
        let mut total_written = 0u64;
        let mut chunks = Vec::new();

        loop {
            let observed_volume = volume_tracker.load(Ordering::Acquire);
            if observed_volume != current_volume {
                if let Some(mut writer) = current_writer.take() {
                    writer.flush().map_err(RarError::Io)?;
                }
                chunks.push((current_volume, current_written));
                current_volume = observed_volume;
                current_writer = Some(writer_factory(current_volume)?);
                current_written = 0;
            }

            let to_read = limit.next_read_len(total_written, buffer.len());
            if to_read == 0 {
                Self::enforce_store_ceiling_not_exceeded(&mut reader, limit, member_name)?;
                break;
            }

            let read = reader.read(&mut buffer[..to_read]).map_err(RarError::Io)?;
            if read == 0 {
                if limit.is_exact() {
                    return Err(RarError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while copying chunked member data",
                    )));
                }
                break;
            }

            if let Some(writer) = current_writer.as_mut() {
                writer.write_all(&buffer[..read]).map_err(RarError::Io)?;
            }
            current_written += read as u64;
            total_written += read as u64;
        }

        if let Some(mut writer) = current_writer {
            writer.flush().map_err(RarError::Io)?;
            if current_written > 0 || chunks.is_empty() {
                chunks.push((current_volume, current_written));
            }
        }

        Ok(chunks)
    }

    /// Extract a member by index, handling any supported compression method.
    ///
    /// For multi-volume archives, this seamlessly reads data across volumes.
    /// Returns the decompressed data as a `Vec<u8>`.
    pub fn extract_member(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
    ) -> RarResult<crate::extract::ExtractedMember> {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        // Check for encryption and resolve password.
        let member_password = if entry.is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: entry.file_header.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };
        let file_enc = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        let fh = entry.file_header.clone();
        let hash = entry.hash.clone();
        let mi = self.member_info(index);
        let is_solid = fh.compression.solid;
        let archive_format = self.format;
        self.enforce_archive_member_limits(&fh)?;
        let unpacked_size = self.target_unpacked_size(&fh);
        let store_limit = self.store_copy_limit(&fh);
        let rar5_crypto = if archive_format == ArchiveFormat::Rar5 {
            member_password
                .as_deref()
                .map(|password| {
                    self.prepare_rar5_encrypted_member(&fh.name, password, file_enc.as_ref())
                })
                .transpose()?
        } else {
            None
        };

        // Report progress start.
        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_start(mi);
        }

        if fh.compression.method == CompressionMethod::Store {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_data_hash = Self::expected_rar14_hash(&fh, options.verify);
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };
            let capacity = self.output_capacity_hint(&fh);
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let tracked_data_hash =
                expected_data_hash.or(expected_crc.map(crate::types::DataHash::Crc32));
            let (written, actual_crc, actual_data_hash, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut hash_writer = HashingWriter::new_with_data_hash(
                    &mut output,
                    tracked_data_hash,
                    expected_blake.is_some(),
                );

                let written = if let Some(ref pwd) = member_password {
                    if archive_format.is_rar4_family() {
                        let reader = Self::wrap_rar4_encrypted_reader(
                            &self.kdf_cache,
                            base_reader,
                            &fh,
                            pwd,
                            rar4_salt,
                        )?;
                        Self::copy_reader_to_writer(
                            reader,
                            &mut hash_writer,
                            store_limit,
                            &fh.name,
                        )?
                    } else {
                        let crypto =
                            rar5_crypto
                                .as_ref()
                                .ok_or_else(|| RarError::CorruptArchive {
                                    detail: format!(
                                        "member {} is missing RAR5 crypto material",
                                        fh.name
                                    ),
                                })?;
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &crypto.key,
                            &crypto.iv,
                        );
                        Self::copy_reader_to_writer(
                            reader,
                            &mut hash_writer,
                            store_limit,
                            &fh.name,
                        )?
                    }
                } else {
                    Self::copy_reader_to_writer(
                        base_reader,
                        &mut hash_writer,
                        store_limit,
                        &fh.name,
                    )?
                };

                (
                    written,
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_data_hash.map(|hash| hash_writer.finalize_data_hash(hash)),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            let use_hash_mac = file_enc.as_ref().is_some_and(|enc| enc.use_hash_mac);
            Self::verify_member_crc32(
                &fh.name,
                expected_crc,
                actual_crc,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_data_hash(
                &fh.name,
                expected_data_hash,
                actual_data_hash,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_blake2(
                &fh.name,
                expected_blake,
                actual_blake,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
            }

            let output = output.into_extracted()?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        if archive_format == ArchiveFormat::Rar5
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };
            let capacity = self.output_capacity_hint(&fh);
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let (actual_crc, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut hash_writer = HashingWriter::new(
                    &mut output,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );

                if let Some(crypto) = rar5_crypto.as_ref() {
                    let reader = crate::crypto::DecryptingReader::new_rar5(
                        base_reader,
                        &crypto.key,
                        &crypto.iv,
                    );
                    let written =
                        crate::decompress::lz::decompress_lz_reader_to_writer_with_max_dict_size(
                            reader,
                            unpacked_size,
                            &fh.compression,
                            &mut hash_writer,
                            self.limits.max_dict_size,
                        )?;
                    self.enforce_unknown_lz_output_limit(&fh, written)?;
                } else {
                    let written =
                        crate::decompress::lz::decompress_lz_reader_to_writer_with_max_dict_size(
                            base_reader,
                            unpacked_size,
                            &fh.compression,
                            &mut hash_writer,
                            self.limits.max_dict_size,
                        )?;
                    self.enforce_unknown_lz_output_limit(&fh, written)?;
                }

                hash_writer.flush().map_err(RarError::Io)?;
                (
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            let use_hash_mac = file_enc.as_ref().is_some_and(|enc| enc.use_hash_mac);
            Self::verify_member_crc32(
                &fh.name,
                expected_crc,
                actual_crc,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_blake2(
                &fh.name,
                expected_blake,
                actual_blake,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, output.len() as u64);
            }

            let output = output.into_extracted()?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        if archive_format.is_rar4_family()
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_data_hash = Self::expected_rar14_hash(&fh, options.verify);
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };
            let capacity = self.output_capacity_hint(&fh);
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let tracked_data_hash =
                expected_data_hash.or(expected_crc.map(crate::types::DataHash::Crc32));
            let (actual_crc, actual_data_hash, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut hash_writer = HashingWriter::new_with_data_hash(
                    &mut output,
                    tracked_data_hash,
                    expected_blake.is_some(),
                );
                if let Some(ref pwd) = member_password {
                    let reader = Self::wrap_rar4_encrypted_reader(
                        &self.kdf_cache,
                        base_reader,
                        &fh,
                        pwd,
                        rar4_salt,
                    )?;
                    let written = crate::decompress::rar4_old::decompress_rar4_reader_to_writer(
                        reader,
                        unpacked_size,
                        fh.compression.version,
                        fh.compression.method.code(),
                        fh.compression.dict_size,
                        &mut hash_writer,
                    )?;
                    self.enforce_unknown_lz_output_limit(&fh, written)?;
                } else {
                    let written = crate::decompress::rar4_old::decompress_rar4_reader_to_writer(
                        base_reader,
                        unpacked_size,
                        fh.compression.version,
                        fh.compression.method.code(),
                        fh.compression.dict_size,
                        &mut hash_writer,
                    )?;
                    self.enforce_unknown_lz_output_limit(&fh, written)?;
                }
                hash_writer.flush().map_err(RarError::Io)?;
                (
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_data_hash.map(|hash| hash_writer.finalize_data_hash(hash)),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            Self::verify_member_crc32(&fh.name, expected_crc, actual_crc, false, None)?;
            Self::verify_member_data_hash(
                &fh.name,
                expected_data_hash,
                actual_data_hash,
                false,
                None,
            )?;
            Self::verify_member_blake2(&fh.name, expected_blake, actual_blake, false, None)?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, output.len() as u64);
            }

            let output = output.into_extracted()?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        if is_solid && fh.compression.method != CompressionMethod::Store {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };
            let capacity = self.output_capacity_hint(&fh);
            let mut output = crate::extract::ExtractedMemberSink::with_capacity_hint(capacity)?;

            let (actual_crc, actual_blake) = {
                let mut hash_writer = HashingWriter::new(
                    &mut output,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );
                self.advance_solid_cursor_to(index, &fh)?;
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                if let Some(ref pwd) = member_password {
                    if archive_format.is_rar4_family() {
                        let reader = Self::wrap_rar4_encrypted_reader(
                            &self.kdf_cache,
                            base_reader,
                            &fh,
                            pwd,
                            rar4_salt,
                        )?;
                        let written = Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?;
                        self.enforce_unknown_lz_output_limit(&fh, written)?;
                    } else {
                        let crypto =
                            rar5_crypto
                                .as_ref()
                                .ok_or_else(|| RarError::CorruptArchive {
                                    detail: format!(
                                        "member {} is missing RAR5 crypto material",
                                        fh.name
                                    ),
                                })?;
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &crypto.key,
                            &crypto.iv,
                        );
                        let written = Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?;
                        self.enforce_unknown_lz_output_limit(&fh, written)?;
                    }
                } else {
                    let written = Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        unpacked_size,
                        &fh,
                        &mut hash_writer,
                    )?;
                    self.enforce_unknown_lz_output_limit(&fh, written)?;
                }
                hash_writer.flush().map_err(RarError::Io)?;
                (
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            let use_hash_mac = file_enc.as_ref().is_some_and(|enc| enc.use_hash_mac);
            Self::verify_member_crc32(
                &fh.name,
                expected_crc,
                actual_crc,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_blake2(
                &fh.name,
                expected_blake,
                actual_blake,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;

            self.solid_next_index = index + 1;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, output.len() as u64);
            }

            let output = output.into_extracted()?;

            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_complete(mi, &Ok(()));
            }

            return Ok(output);
        }

        Err(RarError::CorruptArchive {
            detail: format!(
                "unsupported extraction path for member {} (method {:?}, solid={is_solid}, format={archive_format:?})",
                fh.name, fh.compression.method,
            ),
        })
    }

    /// Extract a member directly to a file, streaming data to disk.
    ///
    /// This is the memory-efficient alternative to `extract_member()`. Instead of
    /// returning the full decompressed content as a `Vec<u8>`, it writes directly
    /// to disk via a `BufWriter`. Memory usage is bounded to dict_size (max 256 MB)
    /// for LZ-compressed files, or ~8 MB for Store (uncompressed) files.
    ///
    /// For Store method without encryption: reads segments directly from volumes
    /// and writes to disk without buffering the full file.
    pub fn extract_member_to_file(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let member_password = if entry.is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: entry.file_header.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        let fh = entry.file_header.clone();
        let member_name = crate::path::sanitize_path(&fh.name);
        let hash = entry.hash.clone();
        let file_enc = entry.file_encryption.clone();
        let redirection = entry.redirection.clone();
        let owner = entry.owner.clone();
        let rar4_salt = entry.rar4_salt;
        let mi = self.member_info(index);
        let is_solid = fh.compression.solid;
        let archive_format = self.format;
        self.enforce_archive_member_limits(&fh)?;
        let unpacked_size = self.target_unpacked_size(&fh);
        let store_limit = self.store_copy_limit(&fh);
        let rar5_crypto = if archive_format == ArchiveFormat::Rar5 {
            member_password
                .as_deref()
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_enc.as_ref()))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_enc.as_ref().is_some_and(|enc| enc.use_hash_mac);

        if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
            p.on_member_start(mi);
        }

        if fh.is_directory {
            Self::ensure_output_parent_dir_for_member(&member_name, out_path)?;
            std::fs::create_dir_all(out_path).map_err(RarError::Io)?;
            Self::apply_output_metadata(&fh, owner.as_ref(), options, out_path)?;
            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, 0);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(0);
        }

        if let Some(written) = self.extract_link_member_to_file(
            index,
            options,
            &fh,
            owner.as_ref(),
            redirection,
            out_path,
        )? {
            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(written);
        }

        // Create output file with buffered writer.
        Self::ensure_output_parent_dir_for_member(&member_name, out_path)?;
        Self::remove_existing_regular_output_symlink(out_path)?;
        let file = std::fs::File::create(out_path).map_err(RarError::Io)?;
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

        if fh.compression.method == CompressionMethod::Store {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_data_hash = Self::expected_rar14_hash(&fh, options.verify);
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };

            let tracked_data_hash =
                expected_data_hash.or(expected_crc.map(crate::types::DataHash::Crc32));
            let (written, actual_crc, actual_data_hash, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let mut hash_writer = HashingWriter::new_with_data_hash(
                    &mut writer,
                    tracked_data_hash,
                    expected_blake.is_some(),
                );

                let written = if let Some(ref pwd) = member_password {
                    if archive_format.is_rar4_family() {
                        let reader = Self::wrap_rar4_encrypted_reader(
                            &self.kdf_cache,
                            base_reader,
                            &fh,
                            pwd,
                            rar4_salt,
                        )?;
                        Self::copy_reader_to_writer(
                            reader,
                            &mut hash_writer,
                            store_limit,
                            &fh.name,
                        )?
                    } else {
                        let crypto =
                            rar5_crypto
                                .as_ref()
                                .ok_or_else(|| RarError::CorruptArchive {
                                    detail: format!(
                                        "member {} is missing RAR5 crypto material",
                                        fh.name
                                    ),
                                })?;
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &crypto.key,
                            &crypto.iv,
                        );
                        Self::copy_reader_to_writer(
                            reader,
                            &mut hash_writer,
                            store_limit,
                            &fh.name,
                        )?
                    }
                } else {
                    Self::copy_reader_to_writer(
                        base_reader,
                        &mut hash_writer,
                        store_limit,
                        &fh.name,
                    )?
                };

                (
                    written,
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_data_hash.map(|hash| hash_writer.finalize_data_hash(hash)),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            Self::verify_member_crc32(
                &fh.name,
                expected_crc,
                actual_crc,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_data_hash(
                &fh.name,
                expected_data_hash,
                actual_data_hash,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_blake2(
                &fh.name,
                expected_blake,
                actual_blake,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;

            writer.flush().map_err(RarError::Io)?;
            Self::apply_output_metadata(&fh, owner.as_ref(), options, out_path)?;
            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(written);
        }

        if archive_format == ArchiveFormat::Rar5
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };

            let segments = self.members[index].segments.clone();
            let base_reader =
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);

            let (actual_crc, actual_blake) = {
                let mut hash_writer = HashingWriter::new(
                    &mut writer,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );
                if let Some(crypto) = rar5_crypto.as_ref() {
                    let reader = crate::crypto::DecryptingReader::new_rar5(
                        base_reader,
                        &crypto.key,
                        &crypto.iv,
                    );
                    let written =
                        crate::decompress::lz::decompress_lz_reader_to_writer_with_max_dict_size(
                            reader,
                            unpacked_size,
                            &fh.compression,
                            &mut hash_writer,
                            self.limits.max_dict_size,
                        )?;
                    self.enforce_unknown_lz_output_limit(&fh, written)?;
                } else {
                    let written =
                        crate::decompress::lz::decompress_lz_reader_to_writer_with_max_dict_size(
                            base_reader,
                            unpacked_size,
                            &fh.compression,
                            &mut hash_writer,
                            self.limits.max_dict_size,
                        )?;
                    self.enforce_unknown_lz_output_limit(&fh, written)?;
                }
                hash_writer.flush().map_err(RarError::Io)?;
                (
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            Self::verify_member_crc32(
                &fh.name,
                expected_crc,
                actual_crc,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_blake2(
                &fh.name,
                expected_blake,
                actual_blake,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;

            Self::apply_output_metadata(&fh, owner.as_ref(), options, out_path)?;
            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, unpacked_size);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(unpacked_size);
        }

        if archive_format.is_rar4_family()
            && fh.compression.method != CompressionMethod::Store
            && !is_solid
        {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_data_hash = Self::expected_rar14_hash(&fh, options.verify);
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };

            let segments = self.members[index].segments.clone();
            let base_reader =
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);

            let tracked_data_hash =
                expected_data_hash.or(expected_crc.map(crate::types::DataHash::Crc32));
            let (written, actual_crc, actual_data_hash, actual_blake) = {
                let mut hash_writer = HashingWriter::new_with_data_hash(
                    &mut writer,
                    tracked_data_hash,
                    expected_blake.is_some(),
                );
                let written = if let Some(ref pwd) = member_password {
                    let reader = Self::wrap_rar4_encrypted_reader(
                        &self.kdf_cache,
                        base_reader,
                        &fh,
                        pwd,
                        rar4_salt,
                    )?;
                    crate::decompress::rar4_old::decompress_rar4_reader_to_writer(
                        reader,
                        unpacked_size,
                        fh.compression.version,
                        fh.compression.method.code(),
                        fh.compression.dict_size,
                        &mut hash_writer,
                    )?
                } else {
                    crate::decompress::rar4_old::decompress_rar4_reader_to_writer(
                        base_reader,
                        unpacked_size,
                        fh.compression.version,
                        fh.compression.method.code(),
                        fh.compression.dict_size,
                        &mut hash_writer,
                    )?
                };
                self.enforce_unknown_lz_output_limit(&fh, written)?;
                hash_writer.flush().map_err(RarError::Io)?;
                let actual_crc = expected_crc.map(|_| hash_writer.finalize_crc());
                let actual_data_hash =
                    expected_data_hash.map(|hash| hash_writer.finalize_data_hash(hash));
                let actual_blake = expected_blake.map(|_| hash_writer.finalize_blake2());
                (written, actual_crc, actual_data_hash, actual_blake)
            };
            writer.flush().map_err(RarError::Io)?;

            Self::verify_member_crc32(&fh.name, expected_crc, actual_crc, false, None)?;
            Self::verify_member_data_hash(
                &fh.name,
                expected_data_hash,
                actual_data_hash,
                false,
                None,
            )?;
            Self::verify_member_blake2(&fh.name, expected_blake, actual_blake, false, None)?;

            Self::apply_output_metadata(&fh, owner.as_ref(), options, out_path)?;
            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(written);
        }

        if is_solid && fh.compression.method != CompressionMethod::Store {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };

            let (written, actual_crc, actual_blake) = {
                let mut hash_writer = HashingWriter::new(
                    &mut writer,
                    expected_crc.is_some(),
                    expected_blake.is_some(),
                );
                self.advance_solid_cursor_to(index, &fh)?;
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name);
                let written = if let Some(ref pwd) = member_password {
                    if archive_format.is_rar4_family() {
                        let reader = Self::wrap_rar4_encrypted_reader(
                            &self.kdf_cache,
                            base_reader,
                            &fh,
                            pwd,
                            rar4_salt,
                        )?;
                        Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?
                    } else {
                        let crypto =
                            rar5_crypto
                                .as_ref()
                                .ok_or_else(|| RarError::CorruptArchive {
                                    detail: format!(
                                        "member {} is missing RAR5 crypto material",
                                        fh.name
                                    ),
                                })?;
                        let reader = crate::crypto::DecryptingReader::new_rar5(
                            base_reader,
                            &crypto.key,
                            &crypto.iv,
                        );
                        Self::solid_decode_reader_to_writer(
                            &mut self.solid_decoder_rar4,
                            &mut self.solid_decoder,
                            self.limits.max_dict_size,
                            reader,
                            unpacked_size,
                            &fh,
                            &mut hash_writer,
                        )?
                    }
                } else {
                    Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        base_reader,
                        unpacked_size,
                        &fh,
                        &mut hash_writer,
                    )?
                };
                self.enforce_unknown_lz_output_limit(&fh, written)?;
                hash_writer.flush().map_err(RarError::Io)?;
                (
                    written,
                    expected_crc.map(|_| hash_writer.finalize_crc()),
                    expected_blake.map(|_| hash_writer.finalize_blake2()),
                )
            };

            Self::verify_member_crc32(
                &fh.name,
                expected_crc,
                actual_crc,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_blake2(
                &fh.name,
                expected_blake,
                actual_blake,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;

            self.solid_next_index = index + 1;

            Self::apply_output_metadata(&fh, owner.as_ref(), options, out_path)?;
            if let (Some(p), Some(mi)) = (progress, mi.as_ref()) {
                p.on_member_progress(mi, written);
                p.on_member_complete(mi, &Ok(()));
            }
            return Ok(written);
        }

        Err(RarError::CorruptArchive {
            detail: format!(
                "unsupported file extraction path for member {} (method {:?}, solid={is_solid}, format={archive_format:?})",
                fh.name, fh.compression.method,
            ),
        })
    }

    /// Extract a solid member into per-volume chunk writers while preserving
    /// the archive's solid decoder state across sequential members.
    pub fn extract_member_solid_chunked<F>(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let member_password = if entry.is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: entry.file_header.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        let fh = entry.file_header.clone();
        let file_enc = entry.file_encryption.clone();
        let hash = entry.hash.clone();
        let rar4_salt = entry.rar4_salt;
        let archive_format = self.format;
        self.enforce_archive_member_limits(&fh)?;
        let unpacked_size = self.target_unpacked_size(&fh);
        let store_limit = self.store_copy_limit(&fh);
        let rar5_crypto = if archive_format == ArchiveFormat::Rar5 {
            member_password
                .as_deref()
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_enc.as_ref()))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_enc.as_ref().is_some_and(|enc| enc.use_hash_mac);

        if !self.is_solid {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "member {} is not in a solid archive; use non-solid chunked extraction",
                    fh.name
                ),
            });
        }

        let segments = self.members[index].segments.clone();

        if fh.compression.method != CompressionMethod::Store {
            let expected_crc = if options.verify { fh.data_crc32 } else { None };
            let expected_blake = if options.verify {
                match hash.as_ref() {
                    Some(FileHash::Blake2sp(expected)) => Some(*expected),
                    _ => None,
                }
            } else {
                None
            };
            let shared_crc =
                expected_crc.map(|_| Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())));
            let shared_blake: Option<Arc<std::sync::Mutex<Blake2spHasher>>> =
                expected_blake.map(|_| Arc::new(std::sync::Mutex::new(Blake2spHasher::new())));
            self.advance_solid_cursor_to(index, &fh)?;

            let volume_tracker = Arc::new(AtomicUsize::new(
                segments.first().map_or(0, |segment| segment.volume_index),
            ));
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let base_reader =
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                    .with_volume_tracker(Arc::clone(&volume_tracker));
            let tracking_reader = VolumeTrackingReader::new(base_reader, volume_tracker)
                .with_shared_transitions(Arc::clone(&shared_transitions));

            let chunks = if let Some(ref pwd) = member_password {
                if archive_format.is_rar4_family() {
                    let reader = Self::wrap_rar4_encrypted_reader(
                        &self.kdf_cache,
                        tracking_reader,
                        &fh,
                        pwd,
                        rar4_salt,
                    )?;
                    let chunks = Self::solid_decode_reader_to_writer_chunked(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        &fh,
                        unpacked_size,
                        segments.first().map_or(0, |segment| segment.volume_index),
                        shared_transitions,
                        writer_factory,
                        shared_crc.clone(),
                        shared_blake.clone(),
                    )?;
                    self.enforce_unknown_lz_chunk_limit(&fh, &chunks)?;
                    chunks
                } else {
                    let crypto = rar5_crypto
                        .as_ref()
                        .ok_or_else(|| RarError::CorruptArchive {
                            detail: format!("member {} is missing RAR5 crypto material", fh.name),
                        })?;
                    let reader = crate::crypto::DecryptingReader::new_rar5(
                        tracking_reader,
                        &crypto.key,
                        &crypto.iv,
                    );
                    let chunks = Self::solid_decode_reader_to_writer_chunked(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        &fh,
                        unpacked_size,
                        segments.first().map_or(0, |segment| segment.volume_index),
                        shared_transitions,
                        writer_factory,
                        shared_crc.clone(),
                        shared_blake.clone(),
                    )?;
                    self.enforce_unknown_lz_chunk_limit(&fh, &chunks)?;
                    chunks
                }
            } else {
                let chunks = Self::solid_decode_reader_to_writer_chunked(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    tracking_reader,
                    &fh,
                    unpacked_size,
                    segments.first().map_or(0, |segment| segment.volume_index),
                    shared_transitions,
                    writer_factory,
                    shared_crc.clone(),
                    shared_blake.clone(),
                )?;
                self.enforce_unknown_lz_chunk_limit(&fh, &chunks)?;
                chunks
            };

            let actual_crc = shared_crc.map(finalize_shared_crc32).transpose()?;
            let actual_blake = shared_blake.map(finalize_shared_blake2).transpose()?;
            Self::verify_member_crc32(
                &fh.name,
                expected_crc,
                actual_crc,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;
            Self::verify_member_blake2(
                &fh.name,
                expected_blake,
                actual_blake,
                use_hash_mac,
                rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
            )?;

            self.solid_next_index = index + 1;
            return Ok(chunks);
        }

        let expected_crc = if options.verify { fh.data_crc32 } else { None };
        let expected_blake = if options.verify {
            match hash.as_ref() {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };

        let volume_tracker = Arc::new(AtomicUsize::new(
            segments.first().map_or(0, |segment| segment.volume_index),
        ));
        let base_reader =
            ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                .with_volume_tracker(Arc::clone(&volume_tracker));

        let shared_crc =
            expected_crc.map(|_| Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())));
        let shared_blake: Option<Arc<std::sync::Mutex<Blake2spHasher>>> =
            expected_blake.map(|_| Arc::new(std::sync::Mutex::new(Blake2spHasher::new())));
        let mut writer_factory = writer_factory;

        let chunks = if let Some(ref pwd) = member_password {
            if archive_format.is_rar4_family() {
                let reader = Self::wrap_rar4_encrypted_reader(
                    &self.kdf_cache,
                    base_reader,
                    &fh,
                    pwd,
                    rar4_salt,
                )?;
                Self::copy_reader_to_writer_chunked(
                    reader,
                    Arc::clone(&volume_tracker),
                    segments.first().map_or(0, |segment| segment.volume_index),
                    |volume_index| {
                        let writer = writer_factory(volume_index)?;
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: shared_crc.as_ref().map(Arc::clone),
                            blake2: shared_blake.as_ref().map(Arc::clone),
                        }))
                    },
                    store_limit,
                    &fh.name,
                )?
            } else {
                let crypto = rar5_crypto
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: format!("member {} is missing RAR5 crypto material", fh.name),
                    })?;
                let reader =
                    crate::crypto::DecryptingReader::new_rar5(base_reader, &crypto.key, &crypto.iv);
                Self::copy_reader_to_writer_chunked(
                    reader,
                    Arc::clone(&volume_tracker),
                    segments.first().map_or(0, |segment| segment.volume_index),
                    |volume_index| {
                        let writer = writer_factory(volume_index)?;
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: shared_crc.as_ref().map(Arc::clone),
                            blake2: shared_blake.as_ref().map(Arc::clone),
                        }))
                    },
                    store_limit,
                    &fh.name,
                )?
            }
        } else {
            Self::copy_reader_to_writer_chunked(
                base_reader,
                Arc::clone(&volume_tracker),
                segments.first().map_or(0, |segment| segment.volume_index),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    Ok(Box::new(HashTrackingWriter {
                        inner: writer,
                        crc: shared_crc.as_ref().map(Arc::clone),
                        blake2: shared_blake.as_ref().map(Arc::clone),
                    }))
                },
                store_limit,
                &fh.name,
            )?
        };

        let actual_crc = shared_crc.map(finalize_shared_crc32).transpose()?;
        let actual_blake = shared_blake.map(finalize_shared_blake2).transpose()?;
        Self::verify_member_crc32(
            &fh.name,
            expected_crc,
            actual_crc,
            use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            expected_blake,
            actual_blake,
            use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        Ok(chunks)
    }
    fn solid_decode_reader_to_sink<R: Read>(
        solid_decoder_rar4: &mut Option<Rar4Decoder>,
        solid_decoder: &mut Option<LzDecoder>,
        max_dict_size: u64,
        compressed: R,
        unpacked_size: u64,
        fh: &FileHeader,
    ) -> RarResult<u64> {
        let mut sink = std::io::sink();
        Self::solid_decode_reader_to_writer(
            solid_decoder_rar4,
            solid_decoder,
            max_dict_size,
            compressed,
            unpacked_size,
            fh,
            &mut sink,
        )
    }

    fn solid_decode_reader_to_writer<R: Read, W: Write>(
        solid_decoder_rar4: &mut Option<Rar4Decoder>,
        solid_decoder: &mut Option<LzDecoder>,
        max_dict_size: u64,
        compressed: R,
        unpacked_size: u64,
        fh: &FileHeader,
        writer: &mut W,
    ) -> RarResult<u64> {
        let dict_size = Self::effective_member_dict_size(fh);
        if dict_size > max_dict_size {
            return Err(RarError::DictionaryTooLarge {
                size: dict_size,
                max: max_dict_size,
            });
        }

        let dict_size = dict_size as usize;
        if fh.compression.format.is_rar4_family() {
            if solid_decoder_rar4
                .as_ref()
                .is_some_and(|decoder| !decoder.supports_version(fh.compression.version))
            {
                *solid_decoder_rar4 = None;
            }
            let decoder = if let Some(decoder) = solid_decoder_rar4 {
                decoder.prepare_solid_continuation();
                decoder
            } else {
                solid_decoder_rar4.insert(Rar4Decoder::new(
                    fh.compression.version,
                    dict_size,
                    fh.compression.method.code(),
                )?)
            };
            decoder.decompress_reader_to_writer(compressed, unpacked_size, writer)
        } else {
            let decoder = if let Some(decoder) = solid_decoder {
                decoder.prepare_solid_continuation();
                decoder
            } else {
                solid_decoder.insert(LzDecoder::try_new(dict_size, fh.compression.version)?)
            };
            decoder.decompress_reader_to_writer(compressed, unpacked_size, writer)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn solid_decode_reader_to_writer_chunked<R: Read, F>(
        solid_decoder_rar4: &mut Option<Rar4Decoder>,
        solid_decoder: &mut Option<LzDecoder>,
        max_dict_size: u64,
        compressed: R,
        fh: &FileHeader,
        unpacked_size: u64,
        first_volume_index: usize,
        shared_transitions: Arc<std::sync::Mutex<Vec<crate::decompress::VolumeTransition>>>,
        writer_factory: F,
        shared_crc: Option<Arc<std::sync::Mutex<crc32fast::Hasher>>>,
        shared_blake: Option<Arc<std::sync::Mutex<Blake2spHasher>>>,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let dict_size = Self::effective_member_dict_size(fh);
        if dict_size > max_dict_size {
            return Err(RarError::DictionaryTooLarge {
                size: dict_size,
                max: max_dict_size,
            });
        }

        let dict_size = dict_size as usize;
        let mut writer_factory = writer_factory;

        let chunks = if fh.compression.format.is_rar4_family() {
            if solid_decoder_rar4
                .as_ref()
                .is_some_and(|decoder| !decoder.supports_version(fh.compression.version))
            {
                *solid_decoder_rar4 = None;
            }
            let decoder = if let Some(decoder) = solid_decoder_rar4 {
                decoder.prepare_solid_continuation();
                decoder
            } else {
                solid_decoder_rar4.insert(Rar4Decoder::new(
                    fh.compression.version,
                    dict_size,
                    fh.compression.method.code(),
                )?)
            };
            let crc_clone = shared_crc.clone();
            let blake_clone = shared_blake.clone();
            decoder.decompress_reader_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                Arc::clone(&shared_transitions),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if crc_clone.is_some() || blake_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: crc_clone.as_ref().map(Arc::clone),
                            blake2: blake_clone.as_ref().map(Arc::clone),
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        } else {
            let decoder = if let Some(decoder) = solid_decoder {
                decoder.prepare_solid_continuation();
                decoder
            } else {
                solid_decoder.insert(LzDecoder::try_new(dict_size, fh.compression.version)?)
            };
            let crc_clone = shared_crc.clone();
            let blake_clone = shared_blake.clone();
            decoder.decompress_reader_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                Arc::clone(&shared_transitions),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if crc_clone.is_some() || blake_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: crc_clone.as_ref().map(Arc::clone),
                            blake2: blake_clone.as_ref().map(Arc::clone),
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        };

        Ok(chunks)
    }
    /// Extract a member by name, handling any supported compression method.
    ///
    /// Returns the decompressed data as a `Vec<u8>`.
    pub fn extract_by_name(
        &mut self,
        name: &str,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
    ) -> RarResult<crate::extract::ExtractedMember> {
        let index = self
            .find_member(name)
            .ok_or_else(|| RarError::MemberNotFound {
                name: name.to_string(),
            })?;
        self.extract_member(index, options, progress)
    }

    /// Extract a member by streaming segments through a [`VolumeProvider`].
    ///
    /// Unlike `extract_member_to_file`, this does not require all volumes to be
    /// present upfront. It reads segments sequentially, requesting each volume
    /// from the provider as needed — which may block until the volume finishes
    /// downloading.
    ///
    /// Currently supports:
    /// - **Store** (uncompressed): streams directly, minimal memory.
    /// - **LZ** (compressed, non-solid): reads compressed data through a
    ///   [`ChainedSegmentReader`], then streams decompressed output.
    /// - **Encrypted** (Store or LZ): wraps the reader in [`DecryptingReader`]
    ///   for on-the-fly AES-CBC decryption.
    ///
    /// Falls back to the buffered path for solid archives.
    pub fn extract_member_streaming<W: Write>(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer: &mut W,
    ) -> RarResult<u64> {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let fh = entry.file_header.clone();
        let is_encrypted = entry.is_encrypted;
        let is_solid = fh.compression.solid;
        let hash = entry.hash.clone();
        let file_encryption = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        self.enforce_archive_member_limits(&fh)?;
        let unpacked_size = self.target_unpacked_size(&fh);

        let member_password = if is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: fh.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        if is_solid {
            return self.extract_member_streaming_solid(
                index,
                &fh,
                hash.as_ref(),
                options,
                provider,
                writer,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                entry.file_header.split_after,
            );
        }

        // For LZ, prefer a reader-backed path when the decoder supports it.
        // ChainedSegmentReader with continuation discovers volumes on demand.
        if fh.compression.method != CompressionMethod::Store {
            let entry = &self.members[index];
            let split_after = entry.file_header.split_after;
            let mut sorted_segs = entry.segments.clone();
            sorted_segs.sort_by_key(|s| s.volume_index);
            // Normalize volume indices to 0-based for the provider.
            // Archive segments use absolute volume numbers (from main header),
            // but the streaming provider uses 0-based local indices.
            let vol_base = sorted_segs.first().map_or(0, |s| s.volume_index);
            for seg in &mut sorted_segs {
                seg.volume_index -= vol_base;
            }

            debug!(
                member = %fh.name,
                method = ?fh.compression.method,
                segments = sorted_segs.len(),
                split_after,
                unpacked_size,
                "streaming LZ extraction starting"
            );

            return self.extract_member_streaming_lz(
                &fh,
                hash.as_ref(),
                options,
                provider,
                &sorted_segs,
                unpacked_size,
                writer,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                split_after,
            );
        }

        // Store mode: stream through ChainedSegmentReader with on-demand volume discovery.
        // One continuous reader → one DecryptingReader → maintains AES-CBC state across volumes.
        let entry = &self.members[index];
        let split_after = entry.file_header.split_after;
        let mut sorted_segs = entry.segments.clone();
        sorted_segs.sort_by_key(|s| s.volume_index);
        // Normalize volume indices to 0-based (see LZ path comment above).
        let vol_base = sorted_segs.first().map_or(0, |s| s.volume_index);
        for seg in &mut sorted_segs {
            seg.volume_index -= vol_base;
        }

        debug!(
            member = %fh.name,
            encrypted = is_encrypted,
            segments = sorted_segs.len(),
            split_after,
            unpacked_size,
            "streaming Store extraction starting"
        );

        self.extract_member_streaming_store(
            &fh,
            hash.as_ref(),
            options,
            provider,
            &sorted_segs,
            unpacked_size,
            writer,
            member_password.as_deref(),
            file_encryption.as_ref(),
            rar4_salt,
            split_after,
        )
    }

    /// Streaming extraction for Store (uncompressed) members.
    ///
    /// Uses `ChainedSegmentReader` with on-demand volume discovery. A single
    /// `DecryptingReader` wraps the entire stream, maintaining continuous
    /// AES-CBC state across volume boundaries.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_solid<W: Write>(
        &mut self,
        index: usize,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer: &mut W,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<u64> {
        let rar5_crypto = if self.format == ArchiveFormat::Rar5 {
            password
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_encryption))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_encryption.is_some_and(|enc| enc.use_hash_mac);
        let (segments, _) = Self::normalized_provider_segments(&self.members[index].segments);

        if fh.compression.method == CompressionMethod::Store {
            return self.extract_member_streaming_store(
                fh,
                hash,
                options,
                provider,
                &segments,
                self.target_unpacked_size(fh),
                writer,
                password,
                file_encryption,
                rar4_salt,
                split_after,
            );
        }

        let expected_crc = if options.verify { fh.data_crc32 } else { None };
        let expected_blake = if options.verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);

        self.advance_solid_cursor_to_streaming(index, fh, provider)?;

        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(&segments, provider)
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta));
        let unpacked_size = self.target_unpacked_size(fh);

        let (written, actual_crc, actual_blake) = {
            let mut hash_writer =
                HashingWriter::new(writer, expected_crc.is_some(), compute_blake2);

            let written = if let Some(pwd) = password {
                if self.format.is_rar4_family() {
                    let reader = Self::wrap_rar4_encrypted_reader(
                        &self.kdf_cache,
                        chained,
                        fh,
                        pwd,
                        rar4_salt,
                    )?;
                    Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        unpacked_size,
                        fh,
                        &mut hash_writer,
                    )?
                } else {
                    let crypto = rar5_crypto
                        .as_ref()
                        .ok_or_else(|| RarError::CorruptArchive {
                            detail: format!("member {} is missing RAR5 crypto material", fh.name),
                        })?;
                    let reader =
                        crate::crypto::DecryptingReader::new_rar5(chained, &crypto.key, &crypto.iv);
                    Self::solid_decode_reader_to_writer(
                        &mut self.solid_decoder_rar4,
                        &mut self.solid_decoder,
                        self.limits.max_dict_size,
                        reader,
                        unpacked_size,
                        fh,
                        &mut hash_writer,
                    )?
                }
            } else {
                Self::solid_decode_reader_to_writer(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    chained,
                    unpacked_size,
                    fh,
                    &mut hash_writer,
                )?
            };
            self.enforce_unknown_lz_output_limit(fh, written)?;

            (
                written,
                expected_crc.map(|_| hash_writer.finalize_crc()),
                compute_blake2.then(|| hash_writer.finalize_blake2()),
            )
        };

        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        Self::verify_member_crc32(
            &fh.name,
            effective_crc,
            actual_crc,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            effective_blake,
            actual_blake,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        self.solid_next_index = index + 1;
        Ok(written)
    }

    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_store<W: Write>(
        &self,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        _unpacked_size: u64,
        writer: &mut W,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<u64> {
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta));
        let rar5_crypto = if self.format == ArchiveFormat::Rar5 {
            password
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_encryption))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_encryption.is_some_and(|enc| enc.use_hash_mac);
        let expected_crc = if options.verify { fh.data_crc32 } else { None };
        let expected_blake = if options.verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);

        // Wrap in DecryptingReader if encrypted, otherwise read directly.
        let mut hasher = if expected_crc.is_some() {
            Some(crc32fast::Hasher::new())
        } else {
            None
        };
        let mut blake_hasher = if compute_blake2 {
            Some(Blake2spHasher::new())
        } else {
            None
        };
        let mut written = 0u64;
        let mut chunk = vec![0u8; STREAMING_STORE_CHUNK_BUFFER_BYTES];

        // For declared Store members, use the unpacked size to avoid copying
        // encrypted padding. Unknown-size members are capped by archive limits.
        let store_limit = self.store_copy_limit(fh);

        let mut reader: Box<dyn Read + '_> = if let Some(pwd) = password {
            if self.format.is_rar4_family() {
                Box::new(Self::wrap_rar4_encrypted_reader(
                    &self.kdf_cache,
                    chained,
                    fh,
                    pwd,
                    rar4_salt,
                )?)
            } else {
                let crypto = rar5_crypto
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: format!("member {} is missing RAR5 crypto material", fh.name),
                    })?;
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &crypto.key,
                    &crypto.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        loop {
            let to_read = store_limit.next_read_len(written, chunk.len());
            if to_read == 0 {
                Self::enforce_store_ceiling_not_exceeded(&mut reader, store_limit, &fh.name)?;
                break;
            }
            let n = reader.read(&mut chunk[..to_read]).map_err(RarError::Io)?;
            if n == 0 {
                break;
            }
            writer.write_all(&chunk[..n]).map_err(RarError::Io)?;
            if let Some(ref mut h) = hasher {
                h.update(&chunk[..n]);
            }
            if let Some(ref mut h) = blake_hasher {
                h.update(&chunk[..n]);
            }
            written += n as u64;
        }

        writer.flush().map_err(RarError::Io)?;

        // Use final volume's CRC and HMAC flag if continuations were discovered.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        let actual_crc = hasher.map(|h| h.finalize());
        let actual_blake = blake_hasher.map(|h| h.finalize());
        Self::verify_member_crc32(
            &fh.name,
            effective_crc,
            actual_crc,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            effective_blake,
            actual_blake,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        Ok(written)
    }

    /// Streaming extraction for LZ-compressed (non-solid) members.
    ///
    /// Uses a `ChainedSegmentReader` with on-demand volume discovery to provide
    /// the compressed bitstream. Volumes are fetched as the decompressor consumes
    /// data. For encrypted members, wraps in `DecryptingReader`.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_lz<W: Write>(
        &self,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        unpacked_size: u64,
        writer: &mut W,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<u64> {
        let rar5_crypto = if self.format == ArchiveFormat::Rar5 {
            password
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_encryption))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_encryption.is_some_and(|enc| enc.use_hash_mac);
        // Build the chained reader first. For RAR5 this can feed the decoder
        // directly; RAR4 still falls back to a buffered compressed-input path
        // because its PPM path depends on contiguous remaining bytes.
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta));

        // Wrap in DecryptingReader if encrypted.
        let inner: Box<dyn Read + '_> = if let Some(pwd) = password {
            if self.format.is_rar4_family() {
                Box::new(Self::wrap_rar4_encrypted_reader(
                    &self.kdf_cache,
                    chained,
                    fh,
                    pwd,
                    rar4_salt,
                )?)
            } else {
                let crypto = rar5_crypto
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: format!("member {} is missing RAR5 crypto material", fh.name),
                    })?;
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &crypto.key,
                    &crypto.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        let expected_crc = if options.verify { fh.data_crc32 } else { None };
        let expected_blake = if options.verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        let mut hash_writer = HashingWriter::new(writer, expected_crc.is_some(), compute_blake2);

        let written = if self.format == ArchiveFormat::Rar5 {
            let mut buf_reader = BufReader::with_capacity(1024 * 1024, inner);
            crate::decompress::lz::decompress_lz_reader_to_writer_with_max_dict_size(
                &mut buf_reader,
                unpacked_size,
                &fh.compression,
                &mut hash_writer,
                self.limits.max_dict_size,
            )?
        } else {
            let mut buf_reader = BufReader::with_capacity(1024 * 1024, inner);
            crate::decompress::rar4_old::decompress_rar4_reader_to_writer(
                &mut buf_reader,
                unpacked_size,
                fh.compression.version,
                fh.compression.method.code(),
                fh.compression.dict_size,
                &mut hash_writer,
            )?
        };
        self.enforce_unknown_lz_output_limit(fh, written)?;

        hash_writer.flush().map_err(RarError::Io)?;

        // Use final volume's CRC and HMAC flag if continuations were discovered.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        let actual_crc = expected_crc.map(|_| hash_writer.finalize_crc());
        let actual_blake = compute_blake2.then(|| hash_writer.finalize_blake2());
        Self::verify_member_crc32(
            &fh.name,
            effective_crc,
            actual_crc,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            effective_blake,
            actual_blake,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        Ok(written)
    }

    /// Extract a member with per-volume output splitting.
    ///
    /// Calls `writer_factory(volume_index)` at each volume transition to get a
    /// new writer. Each writer receives that volume's decompressed contribution.
    /// Returns `Vec<(volume_index, bytes_written)>` for each chunk.
    ///
    /// For Store mode: detects volume transitions via the volume tracker and
    /// switches writers at each boundary.
    ///
    /// For LZ mode: wraps the compressed reader in a `VolumeTrackingReader`
    /// to record compressed byte offsets at volume transitions, then uses
    /// `decompress_to_writer_chunked` to split output at those boundaries.
    pub fn extract_member_streaming_chunked<F>(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer_factory: F,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let entry = self
            .members
            .get(index)
            .ok_or_else(|| RarError::CorruptArchive {
                detail: format!("member index {index} out of range"),
            })?;

        let fh = entry.file_header.clone();
        let is_encrypted = entry.is_encrypted;
        let is_solid = fh.compression.solid;
        let hash = entry.hash.clone();
        let file_encryption = entry.file_encryption.clone();
        let rar4_salt = entry.rar4_salt;
        self.enforce_archive_member_limits(&fh)?;
        let unpacked_size = self.target_unpacked_size(&fh);

        let member_password = if is_encrypted {
            let pwd = options
                .password
                .as_deref()
                .or(self.password.as_deref())
                .ok_or_else(|| RarError::EncryptedMember {
                    member: fh.name.clone(),
                })?;
            Some(pwd.to_string())
        } else {
            None
        };

        if is_solid {
            return self.extract_member_streaming_solid_chunked(
                index,
                &fh,
                hash.as_ref(),
                options,
                provider,
                writer_factory,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                entry.file_header.split_after,
            );
        }

        let entry = &self.members[index];
        let split_after = entry.file_header.split_after;
        let mut sorted_segs = entry.segments.clone();
        sorted_segs.sort_by_key(|s| s.volume_index);
        let vol_base = sorted_segs.first().map_or(0, |s| s.volume_index);
        for seg in &mut sorted_segs {
            seg.volume_index -= vol_base;
        }
        let first_vol = sorted_segs.first().map_or(0, |s| s.volume_index);

        if fh.compression.method != CompressionMethod::Store {
            debug!(
                member = %fh.name,
                method = ?fh.compression.method,
                segments = sorted_segs.len(),
                split_after,
                unpacked_size,
                "streaming chunked LZ extraction starting"
            );
            return self.extract_member_streaming_lz_chunked(
                &fh,
                hash.as_ref(),
                options,
                provider,
                &sorted_segs,
                unpacked_size,
                first_vol,
                writer_factory,
                member_password.as_deref(),
                file_encryption.as_ref(),
                rar4_salt,
                split_after,
            );
        }

        debug!(
            member = %fh.name,
            encrypted = is_encrypted,
            segments = sorted_segs.len(),
            split_after,
            unpacked_size,
            "streaming chunked Store extraction starting"
        );
        self.extract_member_streaming_store_chunked(
            &fh,
            hash.as_ref(),
            options,
            provider,
            &sorted_segs,
            unpacked_size,
            first_vol,
            writer_factory,
            member_password.as_deref(),
            file_encryption.as_ref(),
            rar4_salt,
            split_after,
        )
    }

    /// Chunked Store extraction: switches writers at volume boundaries.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_store_chunked<F>(
        &self,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        _unpacked_size: u64,
        first_vol: usize,
        mut writer_factory: F,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let rar5_crypto = if self.format == ArchiveFormat::Rar5 {
            password
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_encryption))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_encryption.is_some_and(|enc| enc.use_hash_mac);
        let volume_tracker = Arc::new(AtomicUsize::new(first_vol));
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta))
            .with_volume_tracker(Arc::clone(&volume_tracker));
        let expected_crc = if options.verify { fh.data_crc32 } else { None };
        let expected_blake = if options.verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        let mut hasher = if expected_crc.is_some() {
            Some(crc32fast::Hasher::new())
        } else {
            None
        };
        let mut blake_hasher = compute_blake2.then(Blake2spHasher::new);

        let store_limit = self.store_copy_limit(fh);

        let mut reader: Box<dyn Read + '_> = if let Some(pwd) = password {
            if self.format.is_rar4_family() {
                Box::new(Self::wrap_rar4_encrypted_reader(
                    &self.kdf_cache,
                    chained,
                    fh,
                    pwd,
                    rar4_salt,
                )?)
            } else {
                let crypto = rar5_crypto
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: format!("member {} is missing RAR5 crypto material", fh.name),
                    })?;
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &crypto.key,
                    &crypto.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        let mut chunks: Vec<(usize, u64)> = Vec::new();
        let mut current_vol = first_vol;
        let mut current_writer = writer_factory(current_vol)?;
        let mut chunk_bytes: u64 = 0;
        let mut total_written = 0u64;
        let mut chunk = vec![0u8; STREAMING_STORE_CHUNK_BUFFER_BYTES];

        loop {
            let to_read = store_limit.next_read_len(total_written, chunk.len());
            if to_read == 0 {
                Self::enforce_store_ceiling_not_exceeded(&mut reader, store_limit, &fh.name)?;
                break;
            }
            let n = reader.read(&mut chunk[..to_read]).map_err(RarError::Io)?;
            if n == 0 {
                break;
            }

            if let Some(ref mut h) = hasher {
                h.update(&chunk[..n]);
            }
            if let Some(ref mut h) = blake_hasher {
                h.update(&chunk[..n]);
            }

            // Check for volume transition.
            let new_vol = volume_tracker.load(Ordering::Acquire);
            if new_vol != current_vol {
                current_writer.flush().map_err(RarError::Io)?;
                chunks.push((current_vol, chunk_bytes));
                current_vol = new_vol;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            }

            current_writer
                .write_all(&chunk[..n])
                .map_err(RarError::Io)?;
            chunk_bytes += n as u64;
            total_written += n as u64;
        }

        current_writer.flush().map_err(RarError::Io)?;
        if chunk_bytes > 0 || chunks.is_empty() {
            chunks.push((current_vol, chunk_bytes));
        }

        // Verify CRC32.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        let actual_crc = hasher.map(|h| h.finalize());
        let actual_blake = blake_hasher.map(|h| h.finalize());
        Self::verify_member_crc32(
            &fh.name,
            effective_crc,
            actual_crc,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            effective_blake,
            actual_blake,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        Ok(chunks)
    }

    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_solid_chunked<F>(
        &mut self,
        index: usize,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        writer_factory: F,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let rar5_crypto = if self.format == ArchiveFormat::Rar5 {
            password
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_encryption))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_encryption.is_some_and(|enc| enc.use_hash_mac);
        let (segments, first_vol) =
            Self::normalized_provider_segments(&self.members[index].segments);

        if fh.compression.method == CompressionMethod::Store {
            return self.extract_member_streaming_store_chunked(
                fh,
                hash,
                options,
                provider,
                &segments,
                self.target_unpacked_size(fh),
                first_vol,
                writer_factory,
                password,
                file_encryption,
                rar4_salt,
                split_after,
            );
        }

        self.advance_solid_cursor_to_streaming(index, fh, provider)?;

        let volume_tracker = Arc::new(AtomicUsize::new(first_vol));
        let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(&segments, provider)
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta))
            .with_volume_tracker(Arc::clone(&volume_tracker));
        let tracking_reader = VolumeTrackingReader::new(chained, volume_tracker)
            .with_shared_transitions(Arc::clone(&shared_transitions));
        let unpacked_size = self.target_unpacked_size(fh);
        let expected_crc = if options.verify { fh.data_crc32 } else { None };
        let expected_blake = if options.verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        let shared_crc =
            expected_crc.map(|_| Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())));
        let shared_blake: Option<Arc<std::sync::Mutex<Blake2spHasher>>> =
            compute_blake2.then(|| Arc::new(std::sync::Mutex::new(Blake2spHasher::new())));

        let chunks = if let Some(pwd) = password {
            if self.format.is_rar4_family() {
                let reader = Self::wrap_rar4_encrypted_reader(
                    &self.kdf_cache,
                    tracking_reader,
                    fh,
                    pwd,
                    rar4_salt,
                )?;
                Self::solid_decode_reader_to_writer_chunked(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    reader,
                    fh,
                    unpacked_size,
                    first_vol,
                    shared_transitions,
                    writer_factory,
                    shared_crc.clone(),
                    shared_blake.clone(),
                )?
            } else {
                let crypto = rar5_crypto
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: format!("member {} is missing RAR5 crypto material", fh.name),
                    })?;
                let reader = crate::crypto::DecryptingReader::new_rar5(
                    tracking_reader,
                    &crypto.key,
                    &crypto.iv,
                );
                Self::solid_decode_reader_to_writer_chunked(
                    &mut self.solid_decoder_rar4,
                    &mut self.solid_decoder,
                    self.limits.max_dict_size,
                    reader,
                    fh,
                    unpacked_size,
                    first_vol,
                    shared_transitions,
                    writer_factory,
                    shared_crc.clone(),
                    shared_blake.clone(),
                )?
            }
        } else {
            Self::solid_decode_reader_to_writer_chunked(
                &mut self.solid_decoder_rar4,
                &mut self.solid_decoder,
                self.limits.max_dict_size,
                tracking_reader,
                fh,
                unpacked_size,
                first_vol,
                shared_transitions,
                writer_factory,
                shared_crc.clone(),
                shared_blake.clone(),
            )?
        };
        self.enforce_unknown_lz_chunk_limit(fh, &chunks)?;

        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        let actual_crc = shared_crc.map(finalize_shared_crc32).transpose()?;
        let actual_blake = shared_blake.map(finalize_shared_blake2).transpose()?;
        Self::verify_member_crc32(
            &fh.name,
            effective_crc,
            actual_crc,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            effective_blake,
            actual_blake,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        self.solid_next_index = index + 1;
        Ok(chunks)
    }

    /// Chunked LZ extraction: records volume transitions during compressed read,
    /// then splits decompressed output at those boundaries.
    #[allow(clippy::too_many_arguments)]
    fn extract_member_streaming_lz_chunked<F>(
        &self,
        fh: &FileHeader,
        hash: Option<&FileHash>,
        options: &ExtractOptions,
        provider: &dyn VolumeProvider,
        segments: &[DataSegment],
        unpacked_size: u64,
        first_vol: usize,
        mut writer_factory: F,
        password: Option<&str>,
        file_encryption: Option<&FileEncryptionInfo>,
        rar4_salt: Option<[u8; 8]>,
        split_after: bool,
    ) -> RarResult<Vec<(usize, u64)>>
    where
        F: FnMut(usize) -> RarResult<Box<dyn Write>>,
    {
        let rar5_crypto = if self.format == ArchiveFormat::Rar5 {
            password
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_encryption))
                .transpose()?
        } else {
            None
        };
        let use_hash_mac = file_encryption.is_some_and(|enc| enc.use_hash_mac);
        let volume_tracker = Arc::new(AtomicUsize::new(first_vol));
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(split_after, self.format, self.password.clone())
            .with_metadata_sink(Rc::clone(&cont_meta))
            .with_volume_tracker(Arc::clone(&volume_tracker));

        // Build reader chain, wrapping in DecryptingReader if encrypted.
        let inner: Box<dyn Read + '_> = if let Some(pwd) = password {
            if self.format.is_rar4_family() {
                Box::new(Self::wrap_rar4_encrypted_reader(
                    &self.kdf_cache,
                    chained,
                    fh,
                    pwd,
                    rar4_salt,
                )?)
            } else {
                let crypto = rar5_crypto
                    .as_ref()
                    .ok_or_else(|| RarError::CorruptArchive {
                        detail: format!("member {} is missing RAR5 crypto material", fh.name),
                    })?;
                Box::new(crate::crypto::DecryptingReader::new_rar5(
                    chained,
                    &crypto.key,
                    &crypto.iv,
                ))
            }
        } else {
            Box::new(chained)
        };

        // Decompress with chunked output splitting.
        let expected_crc = if options.verify { fh.data_crc32 } else { None };
        let expected_blake = if options.verify {
            match hash {
                Some(FileHash::Blake2sp(expected)) => Some(*expected),
                _ => None,
            }
        } else {
            None
        };
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        let shared_crc: Option<Arc<std::sync::Mutex<crc32fast::Hasher>>> =
            expected_crc.map(|_| Arc::new(std::sync::Mutex::new(crc32fast::Hasher::new())));
        let shared_blake: Option<Arc<std::sync::Mutex<Blake2spHasher>>> =
            compute_blake2.then(|| Arc::new(std::sync::Mutex::new(Blake2spHasher::new())));

        let chunks = if self.format == ArchiveFormat::Rar5 {
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let tracking_reader = VolumeTrackingReader::new(
                BufReader::with_capacity(1024 * 1024, inner),
                volume_tracker,
            )
            .with_shared_transitions(Arc::clone(&shared_transitions));

            let crc_clone = shared_crc.clone();
            let blake_clone = shared_blake.clone();
            crate::decompress::lz::decompress_lz_reader_to_writer_chunked_with_max_dict_size(
                tracking_reader,
                unpacked_size,
                &fh.compression,
                first_vol,
                shared_transitions,
                |vol_idx| {
                    let writer = writer_factory(vol_idx)?;
                    if crc_clone.is_some() || blake_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: crc_clone.as_ref().map(Arc::clone),
                            blake2: blake_clone.as_ref().map(Arc::clone),
                        }))
                    } else {
                        Ok(writer)
                    }
                },
                self.limits.max_dict_size,
            )?
        } else {
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let tracking_reader = VolumeTrackingReader::new(
                BufReader::with_capacity(1024 * 1024, inner),
                volume_tracker,
            )
            .with_shared_transitions(Arc::clone(&shared_transitions));

            let crc_clone = shared_crc.clone();
            let blake_clone = shared_blake.clone();
            crate::decompress::rar4_old::decompress_rar4_reader_to_writer_chunked(
                tracking_reader,
                unpacked_size,
                fh.compression.version,
                fh.compression.method.code(),
                fh.compression.dict_size,
                first_vol,
                shared_transitions,
                |vol_idx| {
                    let writer = writer_factory(vol_idx)?;
                    if crc_clone.is_some() || blake_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            crc: crc_clone.as_ref().map(Arc::clone),
                            blake2: blake_clone.as_ref().map(Arc::clone),
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        };
        self.enforce_unknown_lz_chunk_limit(fh, &chunks)?;

        // Verify CRC32.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        let actual_crc = shared_crc.map(finalize_shared_crc32).transpose()?;
        let actual_blake = shared_blake.map(finalize_shared_blake2).transpose()?;
        Self::verify_member_crc32(
            &fh.name,
            effective_crc,
            actual_crc,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;
        Self::verify_member_blake2(
            &fh.name,
            effective_blake,
            actual_blake,
            final_use_hash_mac,
            rar5_crypto.as_ref().map(|crypto| &crypto.hash_key),
        )?;

        Ok(chunks)
    }
}

/// Writer wrapper that updates a shared CRC hasher.
struct HashingWriter<'a, W: Write> {
    inner: &'a mut W,
    crc: Option<crc32fast::Hasher>,
    rar14: Option<u16>,
    blake2: Option<Blake2spHasher>,
}

impl<'a, W: Write> HashingWriter<'a, W> {
    fn new(inner: &'a mut W, compute_crc: bool, compute_blake2: bool) -> Self {
        Self {
            inner,
            crc: compute_crc.then(crc32fast::Hasher::new),
            rar14: None,
            blake2: compute_blake2.then(Blake2spHasher::new),
        }
    }

    fn new_with_data_hash(
        inner: &'a mut W,
        expected_hash: Option<crate::types::DataHash>,
        compute_blake2: bool,
    ) -> Self {
        Self {
            inner,
            crc: matches!(expected_hash, Some(crate::types::DataHash::Crc32(_)))
                .then(crc32fast::Hasher::new),
            rar14: matches!(expected_hash, Some(crate::types::DataHash::Rar14(_))).then_some(0),
            blake2: compute_blake2.then(Blake2spHasher::new),
        }
    }

    fn finalize_crc(&self) -> u32 {
        self.crc
            .as_ref()
            .map(|hasher| hasher.clone().finalize())
            .unwrap_or(0)
    }

    fn finalize_data_hash(&self, expected: crate::types::DataHash) -> crate::types::DataHash {
        match expected {
            crate::types::DataHash::Crc32(_) => crate::types::DataHash::Crc32(self.finalize_crc()),
            crate::types::DataHash::Rar14(_) => {
                crate::types::DataHash::Rar14(self.rar14.unwrap_or(0))
            }
        }
    }

    fn finalize_blake2(&self) -> [u8; 32] {
        self.blake2
            .as_ref()
            .map(|hasher| hasher.finalize())
            .unwrap_or([0; 32])
    }
}

impl<W: Write> Write for HashingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        if let Some(ref mut hasher) = self.crc {
            hasher.update(&buf[..written]);
        }
        if let Some(ref mut checksum) = self.rar14 {
            *checksum = crate::rar4::header::checksum14_update(*checksum, &buf[..written]);
        }
        if let Some(ref mut hasher) = self.blake2 {
            hasher.update(&buf[..written]);
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Writer wrapper that updates shared hashes while forwarding writes.
struct HashTrackingWriter<W: Write> {
    inner: W,
    crc: Option<Arc<std::sync::Mutex<crc32fast::Hasher>>>,
    blake2: Option<Arc<std::sync::Mutex<Blake2spHasher>>>,
}

impl<W: Write> Write for HashTrackingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if let Some(ref hasher) = self.crc {
            hasher
                .lock()
                .map_err(|_| std::io::Error::other("RAR CRC tracker mutex poisoned"))?
                .update(&buf[..n]);
        }
        if let Some(ref hasher) = self.blake2 {
            hasher
                .lock()
                .map_err(|_| std::io::Error::other("RAR BLAKE2 tracker mutex poisoned"))?
                .update(&buf[..n]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn finalize_shared_crc32(shared: Arc<std::sync::Mutex<crc32fast::Hasher>>) -> RarResult<u32> {
    let mutex = Arc::try_unwrap(shared).map_err(|_| RarError::CorruptArchive {
        detail: "RAR CRC tracker still has active writers after extraction".into(),
    })?;
    let hasher = mutex.into_inner().map_err(|_| RarError::CorruptArchive {
        detail: "RAR CRC tracker mutex poisoned".into(),
    })?;
    Ok(hasher.finalize())
}

fn finalize_shared_blake2(shared: Arc<std::sync::Mutex<Blake2spHasher>>) -> RarResult<[u8; 32]> {
    let mutex = Arc::try_unwrap(shared).map_err(|_| RarError::CorruptArchive {
        detail: "RAR BLAKE2 tracker still has active writers after extraction".into(),
    })?;
    let hasher = mutex.into_inner().map_err(|_| RarError::CorruptArchive {
        detail: "RAR BLAKE2 tracker mutex poisoned".into(),
    })?;
    Ok(hasher.finalize())
}

/// Metadata captured from continuation headers discovered during streaming.
///
/// As `ChainedSegmentReader` discovers continuation volumes, it updates
/// this with the latest header's CRC and encryption flags. After all
/// segments are consumed, the values here are from the final volume —
/// which is authoritative for whole-file CRC verification.
#[derive(Debug, Clone, Default)]
pub(super) struct ContinuationMetadata {
    /// CRC32 from the most recently discovered continuation header.
    data_crc32: Option<u32>,
    /// BLAKE2sp from the most recently discovered continuation header.
    blake2_hash: Option<[u8; 32]>,
    /// Whether the most recently discovered continuation has HASHMAC set.
    use_hash_mac: bool,
}

/// A `Read` adapter that chains data segments across volumes.
///
/// When the current segment is exhausted, it fetches the next volume from the
/// `VolumeProvider` — which may block if that volume hasn't finished downloading.
///
/// If `split_after` is true, when all known segments are consumed it will
/// fetch the next volume from the provider, parse its headers to discover
/// the continuation segment, and keep reading. This enables incremental
/// extraction — bytes flow to the output as each volume arrives.
pub struct ChainedSegmentReader<'a> {
    segments: Vec<DataSegment>,
    provider: &'a dyn VolumeProvider,
    max_data_segment: u64,
    current_seg: usize,
    current_reader: Option<Box<dyn ReadSeek>>,
    remaining_in_segment: u64,
    /// Whether the member continues into more volumes.
    split_after: bool,
    /// Next volume index to discover.
    next_discover_vol: usize,
    /// Archive format (needed to parse continuation headers).
    format: ArchiveFormat,
    /// Password for encrypted header parsing.
    password: Option<String>,
    /// Shared metadata sink updated when continuation headers are discovered.
    /// The caller holds an Rc clone to read final values after streaming.
    metadata_sink: Option<Rc<RefCell<ContinuationMetadata>>>,
    /// Shared volume index tracker. Updated whenever the reader advances to a
    /// new segment, allowing callers (even behind wrapper layers like
    /// DecryptingReader) to observe which volume is currently being read.
    volume_tracker: Option<Arc<AtomicUsize>>,
}

struct ArchiveSegmentReader<'a> {
    volumes: &'a mut [Option<VolumeData>],
    limits: &'a Limits,
    segments: Vec<DataSegment>,
    member_name: String,
    current_seg: usize,
    remaining_in_segment: u64,
    volume_tracker: Option<Arc<AtomicUsize>>,
}

impl<'a> ArchiveSegmentReader<'a> {
    fn new(
        volumes: &'a mut [Option<VolumeData>],
        limits: &'a Limits,
        segments: &[DataSegment],
        member_name: &str,
    ) -> Self {
        let mut sorted = segments.to_vec();
        sorted.sort_by_key(|segment| segment.volume_index);
        Self {
            volumes,
            limits,
            segments: sorted,
            member_name: member_name.to_string(),
            current_seg: 0,
            remaining_in_segment: 0,
            volume_tracker: None,
        }
    }

    fn with_volume_tracker(mut self, tracker: Arc<AtomicUsize>) -> Self {
        self.volume_tracker = Some(tracker);
        self
    }

    fn advance_segment(&mut self) -> std::io::Result<bool> {
        if self.current_seg >= self.segments.len() {
            return Ok(false);
        }

        let seg = &self.segments[self.current_seg];
        if seg.data_size > self.limits.max_data_segment {
            return Err(std::io::Error::other(format!(
                "data segment size {} exceeds limit {}",
                seg.data_size, self.limits.max_data_segment
            )));
        }

        let vol = self
            .volumes
            .get_mut(seg.volume_index)
            .and_then(|v| v.as_mut())
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "missing volume {} for member {}",
                    seg.volume_index, self.member_name
                ))
            })?;

        vol.reader.seek(SeekFrom::Start(seg.data_offset))?;
        if let Some(ref tracker) = self.volume_tracker {
            tracker.store(seg.volume_index, Ordering::Release);
        }
        self.remaining_in_segment = seg.data_size;
        self.current_seg += 1;
        Ok(true)
    }
}

impl Read for ArchiveSegmentReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.remaining_in_segment > 0 {
                let seg = &self.segments[self.current_seg - 1];
                let vol = self
                    .volumes
                    .get_mut(seg.volume_index)
                    .and_then(|v| v.as_mut())
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "missing volume {} for member {}",
                            seg.volume_index, self.member_name
                        ))
                    })?;
                let to_read = buf.len().min(self.remaining_in_segment as usize);
                let n = vol.reader.read(&mut buf[..to_read])?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF in archive volume segment",
                    ));
                }
                self.remaining_in_segment -= n as u64;
                return Ok(n);
            }

            if !self.advance_segment()? {
                return Ok(0);
            }
        }
    }
}

impl<'a> ChainedSegmentReader<'a> {
    pub fn new(segments: &[DataSegment], provider: &'a dyn VolumeProvider) -> Self {
        let next_vol = segments.iter().map(|s| s.volume_index).max().unwrap_or(0) + 1;
        Self {
            segments: segments.to_vec(),
            provider,
            max_data_segment: crate::limits::Limits::default().max_data_segment,
            current_seg: 0,
            current_reader: None,
            remaining_in_segment: 0,
            split_after: false,
            next_discover_vol: next_vol,
            format: ArchiveFormat::Rar5,
            password: None,
            metadata_sink: None,
            volume_tracker: None,
        }
    }

    pub fn with_max_data_segment(mut self, max_data_segment: u64) -> Self {
        self.max_data_segment = max_data_segment;
        self
    }

    /// Enable on-demand volume discovery for multi-volume members.
    pub fn with_continuation(
        mut self,
        split_after: bool,
        format: ArchiveFormat,
        password: Option<String>,
    ) -> Self {
        self.split_after = split_after;
        self.format = format;
        self.password = password;
        self
    }

    /// Attach a metadata sink that receives CRC/encryption info from continuation headers.
    pub fn with_metadata_sink(mut self, sink: Rc<RefCell<ContinuationMetadata>>) -> Self {
        self.metadata_sink = Some(sink);
        self
    }

    /// Attach a shared volume tracker that is updated with the current
    /// volume index each time the reader advances to a new segment.
    pub fn with_volume_tracker(mut self, tracker: Arc<AtomicUsize>) -> Self {
        self.volume_tracker = Some(tracker);
        self
    }

    fn advance_segment(&mut self) -> std::io::Result<bool> {
        if self.current_seg >= self.segments.len() {
            if !self.split_after {
                return Ok(false);
            }
            // Discover the next volume's continuation segment.
            if !self.discover_next_segment()? {
                return Ok(false);
            }
        }

        let seg = &self.segments[self.current_seg];
        if seg.data_size > self.max_data_segment {
            return Err(std::io::Error::other(format!(
                "data segment size {} exceeds limit {}",
                seg.data_size, self.max_data_segment
            )));
        }
        if let Some(ref tracker) = self.volume_tracker {
            tracker.store(seg.volume_index, Ordering::Release);
        }
        let mut reader = self
            .provider
            .get_volume(seg.volume_index)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        reader
            .seek(SeekFrom::Start(seg.data_offset))
            .map_err(std::io::Error::other)?;
        self.current_reader = Some(reader);
        self.remaining_in_segment = seg.data_size;
        self.current_seg += 1;
        Ok(true)
    }

    /// Fetch the next volume, parse its headers, and extract the continuation segment.
    fn discover_next_segment(&mut self) -> std::io::Result<bool> {
        let vol_idx = self.next_discover_vol;
        let mut reader = self
            .provider
            .get_volume(vol_idx)
            .map_err(|e| std::io::Error::other(format!("volume {vol_idx}: {e}")))?;

        reader
            .seek(SeekFrom::Start(0))
            .map_err(std::io::Error::other)?;
        let format = crate::signature::read_signature(&mut reader)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        if format.is_rar4_family() {
            // RAR4: parse headers to find the continuation file entry.
            let parsed = crate::rar4::parse_rar4_headers(&mut reader, self.password.as_deref())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            // Find the first file with split_before (continuation).
            for fh in &parsed.files {
                if fh.split_before {
                    if fh.packed_size > self.max_data_segment {
                        return Err(std::io::Error::other(format!(
                            "data segment size {} exceeds limit {}",
                            fh.packed_size, self.max_data_segment
                        )));
                    }
                    self.segments.push(DataSegment {
                        volume_index: vol_idx,
                        data_offset: fh.data_offset,
                        data_size: fh.packed_size,
                    });
                    self.split_after = fh.split_after;
                    self.next_discover_vol = vol_idx + 1;
                    if let Some(ref sink) = self.metadata_sink {
                        let mut meta = sink.borrow_mut();
                        meta.data_crc32 = Some(fh.crc32);
                        meta.blake2_hash = None;
                        meta.use_hash_mac = false; // RAR4 has no HMAC
                    }
                    return Ok(true);
                }
            }
        } else {
            // RAR5: parse headers.
            let parsed = crate::header::parse_all_headers(&mut reader, self.password.as_deref())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            // Find the first file header with split_before.
            for pf in &parsed.files {
                if pf.header.split_before {
                    if pf.header.data_size > self.max_data_segment {
                        return Err(std::io::Error::other(format!(
                            "data segment size {} exceeds limit {}",
                            pf.header.data_size, self.max_data_segment
                        )));
                    }
                    self.segments.push(DataSegment {
                        volume_index: vol_idx,
                        data_offset: pf.header.data_offset,
                        data_size: pf.header.data_size,
                    });
                    self.split_after = pf.header.split_after;
                    self.next_discover_vol = vol_idx + 1;
                    if let Some(ref sink) = self.metadata_sink {
                        let mut meta = sink.borrow_mut();
                        meta.data_crc32 = pf.header.data_crc32;
                        meta.blake2_hash = pf.hash.as_ref().map(|hash| match hash {
                            FileHash::Blake2sp(value) => *value,
                        });
                        meta.use_hash_mac = pf
                            .file_encryption
                            .as_ref()
                            .is_some_and(|fe| fe.use_hash_mac);
                    }
                    return Ok(true);
                }
            }
        }

        // No continuation found — member is complete.
        self.split_after = false;
        Ok(false)
    }
}

impl Read for ChainedSegmentReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.remaining_in_segment > 0 {
                let reader = self.current_reader.as_mut().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "RAR segment reader missing current volume",
                    )
                })?;
                let to_read = buf.len().min(self.remaining_in_segment as usize);
                let n = reader.read(&mut buf[..to_read])?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected EOF in volume segment",
                    ));
                }
                self.remaining_in_segment -= n as u64;
                return Ok(n);
            }

            // Advance to next segment (may block on VolumeProvider).
            if !self.advance_segment()? {
                return Ok(0); // All segments consumed — EOF.
            }
        }
    }
}

/// A `Read` wrapper that monitors a shared volume tracker and records
/// compressed byte offsets at each volume transition.
///
/// Used in the LZ extraction path: wraps the reader chain (ChainedSegmentReader
/// + optional DecryptingReader), runs `read_to_end`, then provides the recorded
///   transitions for splitting decompressed output at volume boundaries.
pub struct VolumeTrackingReader<R: Read> {
    inner: R,
    volume_tracker: Arc<AtomicUsize>,
    bytes_read: u64,
    last_volume: usize,
    transitions: Vec<crate::decompress::VolumeTransition>,
    shared_transitions: Option<Arc<std::sync::Mutex<Vec<crate::decompress::VolumeTransition>>>>,
}

impl<R: Read> VolumeTrackingReader<R> {
    pub fn new(inner: R, volume_tracker: Arc<AtomicUsize>) -> Self {
        let initial_vol = volume_tracker.load(Ordering::Acquire);
        Self {
            inner,
            volume_tracker,
            bytes_read: 0,
            last_volume: initial_vol,
            transitions: Vec::new(),
            shared_transitions: None,
        }
    }

    pub fn with_shared_transitions(
        mut self,
        transitions: Arc<std::sync::Mutex<Vec<crate::decompress::VolumeTransition>>>,
    ) -> Self {
        self.shared_transitions = Some(transitions);
        self
    }
}

impl<R: Read> Read for VolumeTrackingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.bytes_read += n as u64;
            let current_vol = self.volume_tracker.load(Ordering::Acquire);
            if current_vol != self.last_volume {
                let transition = crate::decompress::VolumeTransition {
                    volume_index: current_vol,
                    compressed_offset: self.bytes_read - n as u64,
                };
                self.transitions.push(transition.clone());
                if let Some(ref shared) = self.shared_transitions
                    && let Ok(mut guard) = shared.lock()
                {
                    guard.push(transition);
                }
                self.last_volume = current_vol;
            }
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ArchiveFormat, CompressionInfo, FileAttributes, HostOs};
    use std::io::{Cursor, Read};
    use std::sync::Arc;

    struct TestVolumeProvider {
        data: Vec<u8>,
    }

    impl crate::volume::VolumeProvider for TestVolumeProvider {
        fn get_volume(
            &self,
            _index: usize,
        ) -> Result<Box<dyn crate::archive::ReadSeek>, crate::volume::VolumeProviderError> {
            Ok(Box::new(Cursor::new(self.data.clone())))
        }
    }

    fn empty_rar5_archive() -> RarArchive {
        RarArchive {
            format: ArchiveFormat::Rar5,
            is_solid: false,
            is_encrypted: false,
            has_recovery_record: false,
            recovery_records: Vec::new(),
            is_locked: false,
            has_authenticity_verification: false,
            has_locator: false,
            quick_open_offset: None,
            recovery_record_offset: None,
            original_name: None,
            original_creation_time: None,
            volume_set: crate::volume::VolumeSet::single(),
            members: Vec::new(),
            services: Vec::new(),
            more_volumes: false,
            volumes: Vec::new(),
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: None,
            kdf_cache: Arc::new(crate::crypto::KdfCache::default()),
        }
    }

    #[test]
    fn chained_segment_reader_rejects_oversized_segment_before_reading() {
        let provider = TestVolumeProvider { data: vec![0; 8] };
        let segments = [DataSegment {
            volume_index: 0,
            data_offset: 0,
            data_size: 9,
        }];
        let mut reader = ChainedSegmentReader::new(&segments, &provider).with_max_data_segment(8);

        let mut buf = [0u8; 1];
        let err = reader.read(&mut buf).unwrap_err();
        assert!(err.to_string().contains("exceeds limit 8"));
    }

    #[test]
    fn archive_member_limits_use_effective_rar4_dictionary_size() {
        let fh = FileHeader {
            name: "rar4-small-dict.bin".into(),
            unpacked_size: Some(0),
            attributes: FileAttributes(0),
            mtime: None,
            ctime: None,
            atime: None,
            data_crc32: None,
            data_hash: None,
            compression: CompressionInfo {
                format: ArchiveFormat::Rar4,
                version: 29,
                solid: false,
                method: CompressionMethod::Normal,
                dict_size: 128 * 1024,
            },
            host_os: HostOs::Unix,
            is_directory: false,
            file_flags: 0,
            data_size: 0,
            split_before: false,
            split_after: false,
            data_offset: 0,
            is_encrypted: false,
            version: None,
            service_subdata: None,
        };

        assert_eq!(RarArchive::effective_member_dict_size(&fh), 0x40000);
    }

    #[test]
    fn archive_member_limits_use_effective_rar5_dictionary_size() {
        let fh = FileHeader {
            name: "rar5-small-dict.bin".into(),
            unpacked_size: Some(0),
            attributes: FileAttributes(0),
            mtime: None,
            ctime: None,
            atime: None,
            data_crc32: None,
            data_hash: None,
            compression: CompressionInfo {
                format: ArchiveFormat::Rar5,
                version: 0,
                solid: false,
                method: CompressionMethod::Normal,
                dict_size: 128 * 1024,
            },
            host_os: HostOs::Unix,
            is_directory: false,
            file_flags: 0,
            data_size: 0,
            split_before: false,
            split_after: false,
            data_offset: 0,
            is_encrypted: false,
            version: None,
            service_subdata: None,
        };

        assert_eq!(RarArchive::effective_member_dict_size(&fh), 0x40000);
    }

    #[test]
    fn rar5_future_encryption_version_is_unsupported_before_kdf() {
        let archive = empty_rar5_archive();
        let result = archive.prepare_rar5_encrypted_member(
            "future.bin",
            "password",
            Some(&FileEncryptionInfo {
                version: 1,
                kdf_count: crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX,
                salt: [0; 16],
                iv: [0; 16],
                check_data: None,
                use_hash_mac: false,
            }),
        );

        match result {
            Err(RarError::UnsupportedEncryption { version: 1 }) => {}
            Err(err) => panic!("expected unsupported encryption version, got {err}"),
            Ok(_) => panic!("expected unsupported encryption version, got success"),
        }
    }

    #[test]
    fn rar5_oversized_encryption_kdf_count_is_unsupported_before_derivation() {
        let archive = empty_rar5_archive();
        let result = archive.prepare_rar5_encrypted_member(
            "expensive.bin",
            "password",
            Some(&FileEncryptionInfo {
                version: 0,
                kdf_count: crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX + 1,
                salt: [0; 16],
                iv: [0; 16],
                check_data: None,
                use_hash_mac: false,
            }),
        );

        match result {
            Err(RarError::UnsupportedEncryptionKdf { count, max })
                if count == crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX + 1
                    && max == crate::crypto::CRYPT5_KDF_LG2_COUNT_MAX => {}
            Err(err) => panic!("expected unsupported encryption KDF count, got {err}"),
            Ok(_) => panic!("expected unsupported encryption KDF count, got success"),
        }
    }
}
