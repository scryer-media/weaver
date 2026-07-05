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
use crate::hash_pipeline::SharedHashStream;
use crate::volume::VolumeProvider;

const STREAMING_STORE_CHUNK_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const MAX_LINK_TARGET_BYTES: usize = 0x10000;
const MAX_COMMENT_BYTES: u64 = 0x1000000;
#[cfg(any(windows, test))]
const WINDOWS_IO_REPARSE_TAG_MOUNT_POINT: u32 = 0xA000_0003;
#[cfg(any(windows, test))]
const WINDOWS_IO_REPARSE_TAG_SYMLINK: u32 = 0xA000_000C;
#[cfg(any(windows, test))]
const WINDOWS_SYMLINK_FLAG_RELATIVE: u32 = 1;
#[cfg(any(windows, test))]
const WINDOWS_FILE_ATTRIBUTE_READONLY: u32 = 0x1;
#[cfg(test)]
const WINDOWS_FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
#[cfg(any(windows, test))]
const WINDOWS_FILE_ATTRIBUTE_DIRECTORY: u32 = 0x10;
#[cfg(any(windows, test))]
const WINDOWS_FILE_ATTRIBUTE_ARCHIVE: u32 = 0x20;
#[cfg(any(windows, test))]
const WINDOWS_OWNER_SECURITY_INFORMATION: u32 = 0x1;
#[cfg(any(windows, test))]
const WINDOWS_GROUP_SECURITY_INFORMATION: u32 = 0x2;
#[cfg(any(windows, test))]
const WINDOWS_DACL_SECURITY_INFORMATION: u32 = 0x4;
#[cfg(any(windows, test))]
const WINDOWS_SACL_SECURITY_INFORMATION: u32 = 0x8;

#[cfg(windows)]
struct WindowsHostMetadata {
    attributes: u32,
    mtime: Option<std::time::SystemTime>,
    ctime: Option<std::time::SystemTime>,
    atime: Option<std::time::SystemTime>,
}

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

#[cfg(all(not(unix), test))]
fn current_umask() -> u32 {
    0
}

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

#[derive(Debug)]
struct Rar3UnixLinkTarget {
    #[cfg_attr(not(unix), allow(dead_code))]
    raw: Vec<u8>,
    safety_target: String,
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
        let comment = if Self::rar4_comment_is_raw_unicode(&service) {
            Self::decode_raw_utf16le_comment(&payload)
        } else {
            Self::decode_rar5_comment_text(&payload)
        };
        Ok((!comment.is_empty()).then_some(comment))
    }

    fn rar4_comment_is_raw_unicode(service: &ServiceEntry) -> bool {
        service.file_header.compression.format.is_rar4_family()
            && service.file_header.name == "CMT"
            && (service.file_header.attributes.0 & 0x0001) != 0
    }

    fn decode_rar5_comment_text(payload: &[u8]) -> String {
        crate::header::common::decode_utf8_prefix_until_nul(payload)
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
        let packed_len = service.segments.iter().try_fold(0u64, |total, segment| {
            total
                .checked_add(segment.data_size)
                .ok_or_else(|| RarError::ResourceLimit {
                    detail: format!("RAR service {} packed size overflows u64", fh.name),
                })
        })?;
        if packed_len == 0 && !fh.split_after {
            return Ok(Vec::new());
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
        let service_password = if service.is_encrypted {
            Some(
                self.password
                    .as_deref()
                    .ok_or_else(|| RarError::EncryptedMember {
                        member: fh.name.clone(),
                    })?
                    .to_owned(),
            )
        } else {
            None
        };
        let rar5_crypto = if service.is_encrypted && fh.compression.format == ArchiveFormat::Rar5 {
            let password =
                service_password
                    .as_deref()
                    .ok_or_else(|| RarError::EncryptedMember {
                        member: fh.name.clone(),
                    })?;
            Some(self.prepare_rar5_encrypted_member(
                &fh.name,
                password,
                service.file_encryption.as_ref(),
            )?)
        } else {
            None
        };

        if service.is_encrypted
            && fh.compression.format != ArchiveFormat::Rar5
            && !fh.compression.format.is_rar4_family()
        {
            return Err(RarError::EncryptedMember {
                member: fh.name.clone(),
            });
        }

        let base_reader =
            ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &service.segments, &fh.name)
                .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
        let reader: Box<dyn Read + '_> = if let Some(crypto) = rar5_crypto.as_ref() {
            Box::new(crate::crypto::DecryptingReader::new_rar5(
                base_reader,
                &crypto.key,
                &crypto.iv,
            ))
        } else if let Some(password) = service_password.as_deref() {
            Box::new(Self::wrap_rar4_encrypted_reader(
                &self.kdf_cache,
                base_reader,
                fh,
                password,
                service.rar4_salt,
            )?)
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
                (output, written, actual_crc, actual_blake)
            } else {
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
                reader
                    .read_to_end(&mut packed)
                    .map_err(Self::read_error_to_rar)?;
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

    fn restore_ntfs_streams_for_member(
        &mut self,
        member_index: usize,
        options: &ExtractOptions,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        let services: Vec<ServiceEntry> = self
            .services
            .iter()
            .filter(|service| service.file_header.name == "STM")
            .filter_map(|service| {
                (self.rar4_service_parent_member_index(service) == Some(member_index))
                    .then_some(service.clone())
            })
            .collect();

        for service in services {
            let Some(stream_name) = Self::ntfs_stream_name_from_service(&service) else {
                continue;
            };
            if Self::is_broken_ntfs_stream_name(&stream_name) {
                if Self::broken_ntfs_stream_name_is_fatal() {
                    return Err(Self::broken_ntfs_stream_name_error(
                        &self.members[member_index].file_header.name,
                        &stream_name,
                    ));
                }
                debug!(
                    member = %self.members[member_index].file_header.name,
                    stream = %stream_name,
                    "skipping broken NTFS alternate stream name on non-Windows target"
                );
                continue;
            }
            if Self::is_prohibited_ntfs_stream_name(&stream_name) {
                debug!(
                    member = %self.members[member_index].file_header.name,
                    stream = %stream_name,
                    "skipping prohibited NTFS alternate stream type"
                );
                continue;
            }
            debug_assert!(Self::is_allowed_ntfs_stream_name(&stream_name));
            self.restore_ntfs_stream(&service, options, out_path, &stream_name)?;
        }

        Ok(())
    }

    fn ntfs_stream_name_from_service(service: &ServiceEntry) -> Option<String> {
        service.ntfs_stream_name.clone().or_else(|| {
            service
                .file_header
                .service_subdata
                .as_deref()
                .map(crate::header::common::decode_utf8_prefix_until_nul)
        })
    }

    fn is_allowed_ntfs_stream_name(stream_name: &str) -> bool {
        !Self::is_broken_ntfs_stream_name(stream_name)
            && !Self::is_prohibited_ntfs_stream_name(stream_name)
    }

    fn is_broken_ntfs_stream_name(stream_name: &str) -> bool {
        !stream_name.starts_with(':') || stream_name.contains(['\\', '/'])
    }

    fn is_prohibited_ntfs_stream_name(stream_name: &str) -> bool {
        stream_name.chars().filter(|ch| *ch == ':').count() > 1
    }

    fn broken_ntfs_stream_name_is_fatal() -> bool {
        cfg!(windows)
    }

    fn broken_ntfs_stream_name_error(member_name: &str, stream_name: &str) -> RarError {
        RarError::CorruptArchive {
            detail: format!(
                "broken NTFS alternate stream name {stream_name:?} for member {member_name:?}"
            ),
        }
    }

    fn restore_ntfs_acl_for_member(
        &mut self,
        member_index: usize,
        options: &ExtractOptions,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        if !options.restore_owners {
            return Ok(());
        }

        let services: Vec<ServiceEntry> = self
            .services
            .iter()
            .filter(|service| service.file_header.name == "ACL")
            .filter_map(|service| {
                (self.rar4_service_parent_member_index(service) == Some(member_index))
                    .then_some(service.clone())
            })
            .collect();

        for service in services {
            self.restore_ntfs_acl(&service, options, out_path)?;
        }

        Ok(())
    }

    #[cfg(windows)]
    fn restore_ntfs_acl(
        &mut self,
        service: &ServiceEntry,
        options: &ExtractOptions,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        let mut payload = Vec::new();
        let written = self.write_service_subdata_to_writer(service, options, &mut payload)?;
        if written == 0 {
            return Ok(());
        }

        let read_sacl = Self::set_ntfs_acl_privileges();
        let security_information = Self::ntfs_acl_security_information(read_sacl);
        let set_ok =
            Self::set_ntfs_acl_security(out_path, security_information, payload.as_mut_ptr());
        if set_ok == 0 {
            debug!(
                member = %service.file_header.name,
                path = %out_path.display(),
                error = %std::io::Error::last_os_error(),
                "could not restore NTFS ACL service data"
            );
        }

        Ok(())
    }

    #[cfg(windows)]
    fn set_ntfs_acl_security(
        out_path: &std::path::Path,
        security_information: u32,
        security_descriptor: *mut u8,
    ) -> i32 {
        let path = Self::windows_path_to_wide_nul(out_path);
        let set_ok = unsafe {
            windows_sys::Win32::Security::SetFileSecurityW(
                path.as_ptr(),
                security_information,
                security_descriptor.cast(),
            )
        };
        if set_ok != 0 {
            return set_ok;
        }

        let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
            return set_ok;
        };
        unsafe {
            windows_sys::Win32::Security::SetFileSecurityW(
                long_path.as_ptr(),
                security_information,
                security_descriptor.cast(),
            )
        }
    }

    #[cfg(windows)]
    fn windows_long_path_to_wide_nul(path: &std::path::Path) -> Option<Vec<u16>> {
        let src = path.as_os_str().to_string_lossy();
        let cur_dir = std::env::current_dir().ok()?;
        let cur_dir = cur_dir.as_os_str().to_string_lossy();
        Self::windows_long_path_string(&src, &cur_dir)
            .map(|path| Self::windows_string_to_wide_nul(&path))
    }

    #[cfg(any(windows, test))]
    fn windows_long_path_string(src: &str, cur_dir: &str) -> Option<String> {
        if src.is_empty() {
            return None;
        }
        let src = src.replace('/', "\\");
        let cur_dir = cur_dir.replace('/', "\\");
        const PREFIX: &str = "\\\\?\\";

        if src.starts_with(PREFIX) {
            return Some(src);
        }
        if Self::windows_path_has_drive_root(&src) {
            return Some(format!("{PREFIX}{src}"));
        }
        if src.starts_with("\\\\") {
            return Some(format!("{PREFIX}UNC{}", &src[1..]));
        }
        if let Some(rest) = src.strip_prefix('\\') {
            let drive = cur_dir.get(..2)?;
            return Some(format!("{PREFIX}{drive}\\{rest}"));
        }

        let relative = src.strip_prefix(".\\").unwrap_or(&src);
        Some(format!(
            "{PREFIX}{}\\{}",
            cur_dir.trim_end_matches('\\'),
            relative
        ))
    }

    #[cfg(any(windows, test))]
    fn windows_path_has_drive_root(path: &str) -> bool {
        path.len() >= 3
            && path.as_bytes()[0].is_ascii_alphabetic()
            && path.as_bytes()[1] == b':'
            && path.as_bytes()[2] == b'\\'
    }

    #[cfg(any(windows, test))]
    fn ntfs_acl_security_information(read_sacl: bool) -> u32 {
        let base = WINDOWS_OWNER_SECURITY_INFORMATION
            | WINDOWS_GROUP_SECURITY_INFORMATION
            | WINDOWS_DACL_SECURITY_INFORMATION;
        if read_sacl {
            base | WINDOWS_SACL_SECURITY_INFORMATION
        } else {
            base
        }
    }

    #[cfg(windows)]
    fn set_ntfs_acl_privileges() -> bool {
        static READ_SACL: OnceLock<bool> = OnceLock::new();
        *READ_SACL.get_or_init(|| {
            let read_sacl =
                Self::enable_windows_privilege(windows_sys::Win32::Security::SE_SECURITY_NAME);
            let _ = Self::enable_windows_privilege(windows_sys::Win32::Security::SE_RESTORE_NAME);
            read_sacl
        })
    }

    #[cfg(windows)]
    fn enable_windows_privilege(privilege_name: windows_sys::core::PCWSTR) -> bool {
        let mut token = std::ptr::null_mut();
        let opened = unsafe {
            windows_sys::Win32::System::Threading::OpenProcessToken(
                windows_sys::Win32::System::Threading::GetCurrentProcess(),
                windows_sys::Win32::Security::TOKEN_ADJUST_PRIVILEGES,
                &mut token,
            )
        };
        if opened == 0 {
            return false;
        }

        struct Token(windows_sys::Win32::Foundation::HANDLE);
        impl Drop for Token {
            fn drop(&mut self) {
                unsafe {
                    windows_sys::Win32::Foundation::CloseHandle(self.0);
                }
            }
        }
        let token = Token(token);

        let mut privileges = windows_sys::Win32::Security::TOKEN_PRIVILEGES {
            PrivilegeCount: 1,
            Privileges: [windows_sys::Win32::Security::LUID_AND_ATTRIBUTES {
                Luid: windows_sys::Win32::Foundation::LUID {
                    LowPart: 0,
                    HighPart: 0,
                },
                Attributes: windows_sys::Win32::Security::SE_PRIVILEGE_ENABLED,
            }],
        };
        let looked_up = unsafe {
            windows_sys::Win32::Security::LookupPrivilegeValueW(
                std::ptr::null(),
                privilege_name,
                &mut privileges.Privileges[0].Luid,
            )
        };
        if looked_up == 0 {
            return false;
        }

        unsafe {
            windows_sys::Win32::Foundation::SetLastError(0);
        }
        let adjusted = unsafe {
            windows_sys::Win32::Security::AdjustTokenPrivileges(
                token.0,
                0,
                &privileges,
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        adjusted != 0 && unsafe { windows_sys::Win32::Foundation::GetLastError() } == 0
    }

    #[cfg(not(windows))]
    fn restore_ntfs_acl(
        &mut self,
        service: &ServiceEntry,
        options: &ExtractOptions,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        let _ = self;
        let _ = service;
        let _ = options;
        let _ = out_path;
        Ok(())
    }

    #[cfg(windows)]
    fn restore_ntfs_stream(
        &mut self,
        service: &ServiceEntry,
        options: &ExtractOptions,
        out_path: &std::path::Path,
        stream_name: &str,
    ) -> RarResult<()> {
        let host_metadata = Self::capture_windows_host_metadata(out_path)?;
        Self::clear_windows_readonly_for_stream_write(out_path, &host_metadata)?;

        let stream_path = Self::ntfs_stream_output_path(out_path, stream_name);
        let write_result = (|| {
            let file = std::fs::File::create(stream_path).map_err(RarError::Io)?;
            let mut writer = BufWriter::new(file);
            self.write_service_subdata_to_writer(service, options, &mut writer)?;
            writer.flush().map_err(RarError::Io)
        })();
        let restore_result = Self::restore_windows_host_metadata(out_path, &host_metadata);
        write_result?;
        restore_result
    }

    #[cfg(windows)]
    fn capture_windows_host_metadata(
        out_path: &std::path::Path,
    ) -> RarResult<Option<WindowsHostMetadata>> {
        let attributes = Self::get_windows_file_attributes(out_path);
        if attributes == u32::MAX {
            return Ok(None);
        }

        let metadata = std::fs::metadata(out_path).map_err(RarError::Io)?;
        Ok(Some(WindowsHostMetadata {
            attributes,
            mtime: metadata.modified().ok(),
            ctime: metadata.created().ok(),
            atime: metadata.accessed().ok(),
        }))
    }

    #[cfg(windows)]
    fn clear_windows_readonly_for_stream_write(
        out_path: &std::path::Path,
        metadata: &Option<WindowsHostMetadata>,
    ) -> RarResult<()> {
        let Some(metadata) = metadata else {
            return Ok(());
        };
        let attributes = Self::windows_attributes_without_readonly(metadata.attributes);
        if attributes == metadata.attributes {
            return Ok(());
        }
        Self::set_windows_file_attributes(out_path, attributes)
    }

    #[cfg(any(windows, test))]
    fn windows_attributes_without_readonly(attributes: u32) -> u32 {
        attributes & !WINDOWS_FILE_ATTRIBUTE_READONLY
    }

    #[cfg(windows)]
    fn restore_windows_host_metadata(
        out_path: &std::path::Path,
        metadata: &Option<WindowsHostMetadata>,
    ) -> RarResult<()> {
        let Some(metadata) = metadata else {
            return Ok(());
        };

        if let Some(mtime) = metadata.mtime {
            filetime::set_file_mtime(out_path, filetime::FileTime::from_system_time(mtime))
                .map_err(RarError::Io)?;
        }
        if let Some(atime) = metadata.atime {
            filetime::set_file_atime(out_path, filetime::FileTime::from_system_time(atime))
                .map_err(RarError::Io)?;
        }
        Self::apply_creation_time(metadata.ctime, out_path)?;
        Self::set_windows_file_attributes(out_path, metadata.attributes)
    }

    #[cfg(windows)]
    fn set_windows_file_attributes(out_path: &std::path::Path, attributes: u32) -> RarResult<()> {
        if Self::set_windows_file_attributes_raw(out_path, attributes) {
            return Ok(());
        }
        Err(RarError::Io(std::io::Error::last_os_error()))
    }

    #[cfg(windows)]
    fn get_windows_file_attributes(out_path: &std::path::Path) -> u32 {
        let path = Self::windows_path_to_wide_nul(out_path);
        let attributes =
            unsafe { windows_sys::Win32::Storage::FileSystem::GetFileAttributesW(path.as_ptr()) };
        if attributes != u32::MAX {
            return attributes;
        }

        let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
            return attributes;
        };
        unsafe { windows_sys::Win32::Storage::FileSystem::GetFileAttributesW(long_path.as_ptr()) }
    }

    #[cfg(windows)]
    fn set_windows_file_attributes_raw(out_path: &std::path::Path, attributes: u32) -> bool {
        let path = Self::windows_path_to_wide_nul(out_path);
        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::SetFileAttributesW(path.as_ptr(), attributes)
        };
        if ok != 0 {
            return true;
        }

        let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
            return false;
        };
        unsafe {
            windows_sys::Win32::Storage::FileSystem::SetFileAttributesW(
                long_path.as_ptr(),
                attributes,
            ) != 0
        }
    }

    #[cfg(not(windows))]
    fn restore_ntfs_stream(
        &mut self,
        service: &ServiceEntry,
        options: &ExtractOptions,
        out_path: &std::path::Path,
        stream_name: &str,
    ) -> RarResult<()> {
        let _ = self;
        let _ = service;
        let _ = options;
        let _ = out_path;
        let _ = stream_name;
        Ok(())
    }

    #[cfg(windows)]
    fn ntfs_stream_output_path(
        out_path: &std::path::Path,
        stream_name: &str,
    ) -> std::path::PathBuf {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStrExt;

        let mut base =
            if out_path.parent().is_none() && out_path.as_os_str().encode_wide().count() == 1 {
                let mut prefixed = OsString::from(".\\");
                prefixed.push(out_path.as_os_str());
                prefixed
            } else {
                out_path.as_os_str().to_os_string()
            };
        base.push(stream_name);
        std::path::PathBuf::from(base)
    }

    #[cfg(any(windows, test))]
    fn write_service_subdata_to_writer(
        &mut self,
        service: &ServiceEntry,
        options: &ExtractOptions,
        writer: &mut dyn Write,
    ) -> RarResult<u64> {
        let fh = &service.file_header;
        let unpacked_size = fh.unpacked_size.ok_or_else(|| RarError::CorruptArchive {
            detail: format!("RAR service {} has unknown unpacked size", fh.name),
        })?;
        let packed_len = service.segments.iter().try_fold(0u64, |total, segment| {
            total
                .checked_add(segment.data_size)
                .ok_or_else(|| RarError::ResourceLimit {
                    detail: format!("RAR service {} packed size overflows u64", fh.name),
                })
        })?;
        if packed_len == 0 && !fh.split_after {
            return Ok(0);
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
        let service_password = if service.is_encrypted {
            Some(
                options
                    .password
                    .as_deref()
                    .or(self.password.as_deref())
                    .ok_or_else(|| RarError::EncryptedMember {
                        member: fh.name.clone(),
                    })?
                    .to_owned(),
            )
        } else {
            None
        };
        let rar5_crypto = if service.is_encrypted && fh.compression.format == ArchiveFormat::Rar5 {
            let password =
                service_password
                    .as_deref()
                    .ok_or_else(|| RarError::EncryptedMember {
                        member: fh.name.clone(),
                    })?;
            Some(self.prepare_rar5_encrypted_member(
                &fh.name,
                password,
                service.file_encryption.as_ref(),
            )?)
        } else {
            None
        };

        if service.is_encrypted
            && fh.compression.format != ArchiveFormat::Rar5
            && !fh.compression.format.is_rar4_family()
        {
            return Err(RarError::EncryptedMember {
                member: fh.name.clone(),
            });
        }

        let base_reader =
            ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &service.segments, &fh.name)
                .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
        let reader: Box<dyn Read + '_> = if let Some(crypto) = rar5_crypto.as_ref() {
            Box::new(crate::crypto::DecryptingReader::new_rar5(
                base_reader,
                &crypto.key,
                &crypto.iv,
            ))
        } else if let Some(password) = service_password.as_deref() {
            Box::new(Self::wrap_rar4_encrypted_reader(
                &self.kdf_cache,
                base_reader,
                fh,
                password,
                service.rar4_salt,
            )?)
        } else {
            Box::new(base_reader)
        };

        let mut hash_writer =
            HashingWriter::new(writer, expected_crc.is_some(), expected_blake.is_some());
        let written = if fh.compression.method == CompressionMethod::Store {
            Self::copy_reader_to_writer(
                reader,
                &mut hash_writer,
                StoreCopyLimit::Exact(unpacked_size),
                &fh.name,
            )?
        } else if fh.compression.format == ArchiveFormat::Rar5 {
            let written = crate::decompress::lz::decompress_lz_reader_to_writer_with_max_dict_size(
                reader,
                unpacked_size,
                &fh.compression,
                &mut hash_writer,
                self.limits.max_dict_size,
            )?;
            self.enforce_unknown_lz_output_limit(fh, written)?;
            written
        } else if fh.compression.format.is_rar4_family() {
            let written = crate::decompress::rar4_old::decompress_rar4_reader_to_writer(
                reader,
                unpacked_size,
                fh.compression.version,
                fh.compression.method.code(),
                fh.compression.dict_size,
                &mut hash_writer,
            )?;
            self.enforce_unknown_lz_output_limit(fh, written)?;
            written
        } else {
            return Err(RarError::UnsupportedFormat {
                version: match fh.compression.format {
                    ArchiveFormat::Rar14 => 14,
                    ArchiveFormat::Rar4 => 4,
                    ArchiveFormat::Rar5 => 5,
                },
            });
        };
        hash_writer.flush().map_err(RarError::Io)?;

        if written != unpacked_size {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "RAR service {} produced {} bytes, expected {}",
                    fh.name, written, unpacked_size
                ),
            });
        }

        let actual_crc = expected_crc.map(|_| hash_writer.finalize_crc());
        let actual_blake = expected_blake.map(|_| hash_writer.finalize_blake2());
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

        Ok(written)
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
    ) -> RarResult<Rar3UnixLinkTarget> {
        if fh.data_size > MAX_LINK_TARGET_BYTES as u64 {
            return Err(RarError::ResourceLimit {
                detail: format!(
                    "RAR3 Unix symlink target for {} is {} bytes, exceeding MAXPATHSIZE {}",
                    fh.name, fh.data_size, MAX_LINK_TARGET_BYTES
                ),
            });
        }

        let link_options = ExtractOptions {
            verify: false,
            password: options.password.clone(),
            restore_owners: false,
        };
        let payload = self
            .extract_member_with_link_policy(index, &link_options, None, true)?
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

        Self::decode_rar3_unix_link_target_payload(fh, &payload, options.verify)
    }

    fn decode_rar3_unix_link_target_payload(
        fh: &FileHeader,
        payload: &[u8],
        verify: bool,
    ) -> RarResult<Rar3UnixLinkTarget> {
        let nul_pos = payload
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(payload.len());
        let target_bytes = &payload[..nul_pos];
        if verify && let Some(expected) = fh.data_crc32 {
            let actual = crc32fast::hash(target_bytes);
            if actual != expected {
                return Err(RarError::DataCrcMismatch {
                    member: fh.name.clone(),
                    expected,
                    actual,
                });
            }
        }

        if target_bytes.is_empty() {
            return Err(RarError::CorruptArchive {
                detail: format!("RAR3 Unix symlink target for {} is empty", fh.name),
            });
        }

        let safety_target = Self::decode_rar3_unix_link_target_for_safety(fh, target_bytes)?;
        Ok(Rar3UnixLinkTarget {
            raw: target_bytes.to_vec(),
            safety_target,
        })
    }

    fn decode_rar3_unix_link_target_for_safety(
        fh: &FileHeader,
        target_bytes: &[u8],
    ) -> RarResult<String> {
        let Some(target) = Self::decode_unrar_utf8_for_link_safety(target_bytes) else {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "RAR3 Unix symlink target for {} is not valid UTF-8",
                    fh.name
                ),
            });
        };

        let raw_path_chars = target_bytes
            .iter()
            .filter(|byte| matches!(byte, b'/' | b'.'))
            .count();
        let decoded_path_chars = target.chars().filter(|ch| matches!(ch, '/' | '.')).count();
        if raw_path_chars != decoded_path_chars {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "RAR3 Unix symlink target for {} changes path metacharacters when decoded",
                    fh.name
                ),
            });
        }

        Ok(target)
    }

    fn decode_unrar_utf8_for_link_safety(bytes: &[u8]) -> Option<String> {
        let mut out = String::new();
        let mut pos = 0usize;
        while pos < bytes.len() {
            let byte = bytes[pos];
            if byte == 0 {
                break;
            }
            pos += 1;

            let decoded = if byte < 0x80 {
                u32::from(byte)
            } else if byte >> 5 == 6 {
                let b0 = *bytes.get(pos)?;
                if b0 & 0xc0 != 0x80 {
                    return None;
                }
                pos += 1;
                (u32::from(byte & 0x1f) << 6) | u32::from(b0 & 0x3f)
            } else if byte >> 4 == 14 {
                let b0 = *bytes.get(pos)?;
                let b1 = *bytes.get(pos + 1)?;
                if b0 & 0xc0 != 0x80 || b1 & 0xc0 != 0x80 {
                    return None;
                }
                pos += 2;
                (u32::from(byte & 0x0f) << 12) | (u32::from(b0 & 0x3f) << 6) | u32::from(b1 & 0x3f)
            } else if byte >> 3 == 30 {
                let b0 = *bytes.get(pos)?;
                let b1 = *bytes.get(pos + 1)?;
                let b2 = *bytes.get(pos + 2)?;
                if b0 & 0xc0 != 0x80 || b1 & 0xc0 != 0x80 || b2 & 0xc0 != 0x80 {
                    return None;
                }
                pos += 3;
                (u32::from(byte & 0x07) << 18)
                    | (u32::from(b0 & 0x3f) << 12)
                    | (u32::from(b1 & 0x3f) << 6)
                    | u32::from(b2 & 0x3f)
            } else {
                return None;
            };

            let ch = if (0xd800..=0xdfff).contains(&decoded) {
                char::REPLACEMENT_CHARACTER
            } else {
                char::from_u32(decoded)?
            };
            out.push(ch);
        }

        Some(out)
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
                Self::clear_windows_readonly_before_delete(out_path)?;
                Self::remove_output_file(out_path)?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(RarError::Io(err)),
        }
        Ok(())
    }

    fn remove_existing_regular_output_symlink(out_path: &std::path::Path) -> RarResult<()> {
        match std::fs::symlink_metadata(out_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                Self::clear_windows_readonly_before_delete(out_path)?;
                Self::remove_output_file(out_path)?;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(RarError::Io(err)),
        }
        Ok(())
    }

    fn clear_windows_readonly_before_delete(out_path: &std::path::Path) -> RarResult<()> {
        Self::clear_windows_readonly_attribute(out_path)
    }

    fn clear_windows_readonly_before_overwrite(out_path: &std::path::Path) -> RarResult<()> {
        Self::clear_windows_readonly_attribute(out_path)
    }

    fn remove_output_file(out_path: &std::path::Path) -> RarResult<()> {
        #[cfg(windows)]
        {
            let path = Self::windows_path_to_wide_nul(out_path);
            let ok = unsafe { windows_sys::Win32::Storage::FileSystem::DeleteFileW(path.as_ptr()) };
            if ok != 0 {
                return Ok(());
            }

            let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
                return Err(RarError::Io(std::io::Error::last_os_error()));
            };
            let ok =
                unsafe { windows_sys::Win32::Storage::FileSystem::DeleteFileW(long_path.as_ptr()) };
            if ok == 0 {
                return Err(RarError::Io(std::io::Error::last_os_error()));
            }
            Ok(())
        }

        #[cfg(not(windows))]
        {
            std::fs::remove_file(out_path).map_err(RarError::Io)
        }
    }

    #[cfg(windows)]
    fn remove_output_dir(out_path: &std::path::Path) -> RarResult<()> {
        let path = Self::windows_path_to_wide_nul(out_path);
        let ok =
            unsafe { windows_sys::Win32::Storage::FileSystem::RemoveDirectoryW(path.as_ptr()) };
        if ok != 0 {
            return Ok(());
        }

        let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
            return Err(RarError::Io(std::io::Error::last_os_error()));
        };
        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::RemoveDirectoryW(long_path.as_ptr())
        };
        if ok == 0 {
            return Err(RarError::Io(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    #[cfg(windows)]
    fn windows_output_path_exists(out_path: &std::path::Path) -> bool {
        Self::get_windows_file_attributes(out_path) != u32::MAX
    }

    #[cfg(windows)]
    fn windows_output_path_is_dir(out_path: &std::path::Path) -> bool {
        let attributes = Self::get_windows_file_attributes(out_path);
        attributes != u32::MAX && attributes & WINDOWS_FILE_ATTRIBUTE_DIRECTORY != 0
    }

    #[cfg(any(windows, test))]
    fn windows_path_ends_with_dot_or_space(out_path: &std::path::Path) -> bool {
        out_path
            .as_os_str()
            .to_string_lossy()
            .chars()
            .next_back()
            .is_some_and(|ch| ch == '.' || ch == ' ')
    }

    fn create_output_dir(out_path: &std::path::Path) -> RarResult<()> {
        #[cfg(windows)]
        {
            let special = Self::windows_path_ends_with_dot_or_space(out_path);
            if !special {
                let path = Self::windows_path_to_wide_nul(out_path);
                let ok = unsafe {
                    windows_sys::Win32::Storage::FileSystem::CreateDirectoryW(
                        path.as_ptr(),
                        std::ptr::null(),
                    )
                };
                if ok != 0 {
                    return Ok(());
                }
                let err = std::io::Error::last_os_error();
                if Self::windows_output_path_exists(out_path) {
                    return Err(RarError::Io(err));
                }
            }

            let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
                return Err(RarError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "could not build long Windows path for {}",
                        out_path.display()
                    ),
                )));
            };
            let ok = unsafe {
                windows_sys::Win32::Storage::FileSystem::CreateDirectoryW(
                    long_path.as_ptr(),
                    std::ptr::null(),
                )
            };
            if ok == 0 {
                return Err(RarError::Io(std::io::Error::last_os_error()));
            }
            Ok(())
        }

        #[cfg(not(windows))]
        {
            std::fs::create_dir(out_path).map_err(RarError::Io)
        }
    }

    fn create_output_dir_all(out_path: &std::path::Path) -> RarResult<()> {
        #[cfg(windows)]
        {
            if out_path.as_os_str().is_empty() || Self::windows_output_path_is_dir(out_path) {
                return Ok(());
            }
            if Self::windows_output_path_exists(out_path) {
                return Err(RarError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "output directory path {} exists and is not a directory",
                        out_path.display()
                    ),
                )));
            }
            if let Some(parent) = out_path.parent()
                && !parent.as_os_str().is_empty()
                && parent != out_path
            {
                Self::create_output_dir_all(parent)?;
            }
            match Self::create_output_dir(out_path) {
                Ok(()) => Ok(()),
                Err(_) if Self::windows_output_path_is_dir(out_path) => Ok(()),
                Err(err) => Err(err),
            }
        }

        #[cfg(not(windows))]
        {
            std::fs::create_dir_all(out_path).map_err(RarError::Io)
        }
    }

    fn clear_windows_readonly_attribute(out_path: &std::path::Path) -> RarResult<()> {
        #[cfg(windows)]
        {
            let attributes = Self::get_windows_file_attributes(out_path);
            if attributes != u32::MAX && attributes & WINDOWS_FILE_ATTRIBUTE_READONLY != 0 {
                Self::set_windows_file_attributes(
                    out_path,
                    Self::windows_attributes_without_readonly(attributes),
                )?;
            }
        }

        #[cfg(not(windows))]
        {
            let _ = out_path;
        }

        Ok(())
    }

    fn ensure_output_parent_dir(out_path: &std::path::Path) -> RarResult<()> {
        if let Some(parent) = out_path.parent()
            && !parent.as_os_str().is_empty()
        {
            Self::create_output_dir_all(parent)?;
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
            Self::create_output_dir_all(&root)?;
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
                    Self::clear_windows_readonly_before_delete(&current)?;
                    Self::remove_output_file(&current)?;
                    Self::create_output_dir(&current)?;
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
                    Self::create_output_dir(&current)?;
                }
                Err(err) => return Err(RarError::Io(err)),
            }
        }

        Ok(())
    }

    fn ensure_output_directory_at_member_path(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        match std::fs::symlink_metadata(out_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                Self::clear_windows_readonly_before_delete(out_path)?;
                Self::remove_output_file(out_path)?;
                Self::create_output_dir(out_path)?;
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(RarError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "directory output path {} exists and is not a directory",
                        out_path.display()
                    ),
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Self::create_output_dir(out_path)?;
            }
            Err(err) => return Err(RarError::Io(err)),
        }
        Ok(())
    }

    fn ensure_output_directory_for_member(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<std::path::PathBuf> {
        match Self::ensure_output_directory_at_member_path(member_name, out_path) {
            Ok(()) => Ok(out_path.to_path_buf()),
            Err(original_err) => {
                if let Some((fallback_member_name, fallback_path)) =
                    Self::unix_windows_share_fallback_output_path(member_name, out_path)
                {
                    tracing::debug!(
                        original = %out_path.display(),
                        fallback = %fallback_path.display(),
                        "retrying directory creation with UnRAR-compatible Windows-share name"
                    );
                    Self::ensure_output_directory_at_member_path(
                        &fallback_member_name,
                        &fallback_path,
                    )?;
                    Ok(fallback_path)
                } else {
                    Err(original_err)
                }
            }
        }
    }

    fn ensure_safe_link_target(
        member_name: &str,
        target: &str,
        raw_member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        if !crate::path::is_safe_symlink_target_for_member(
            member_name,
            raw_member_name,
            target,
            true,
        ) {
            return Err(RarError::UnsafeLinkTarget {
                member: raw_member_name.to_string(),
                target: target.to_string(),
            });
        }
        if Self::link_target_has_parent_traversal(target, true)
            && Self::source_path_has_existing_link_or_non_dir(member_name, out_path)?
        {
            return Err(RarError::UnsafeLinkTarget {
                member: raw_member_name.to_string(),
                target: target.to_string(),
            });
        }
        Ok(())
    }

    fn ensure_safe_symlink_target(
        fh: &FileHeader,
        member_name: &str,
        target: &str,
        raw_member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        if !crate::path::is_safe_symlink_target_for_archive_member(
            fh.compression.format,
            member_name,
            raw_member_name,
            target,
        ) {
            return Err(RarError::UnsafeLinkTarget {
                member: raw_member_name.to_string(),
                target: target.to_string(),
            });
        }
        if Self::link_target_has_parent_traversal(target, false)
            && Self::source_path_has_existing_link_or_non_dir(member_name, out_path)?
        {
            return Err(RarError::UnsafeLinkTarget {
                member: raw_member_name.to_string(),
                target: target.to_string(),
            });
        }
        Ok(())
    }

    fn link_target_has_parent_traversal(target: &str, backslash_is_separator: bool) -> bool {
        if backslash_is_separator {
            target
                .replace('\\', "/")
                .split('/')
                .any(|part| part == "..")
        } else {
            target.split('/').any(|part| part == "..")
        }
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

    fn normalize_file_redirection_target(target: &str) -> String {
        crate::path::sanitize_file_redirection_path(target)
    }

    fn normalized_rar5_redirection_target(
        redir_type: header::RedirectionType,
        target: &str,
    ) -> String {
        match redir_type {
            header::RedirectionType::UnixSymlink => target.to_string(),
            header::RedirectionType::WindowsSymlink | header::RedirectionType::WindowsJunction => {
                Self::normalize_windows_redirection_target(target)
            }
            header::RedirectionType::Hardlink | header::RedirectionType::FileCopy => {
                Self::normalize_file_redirection_target(target)
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
            .join(crate::path::sanitize_file_redirection_path(target))
    }

    fn unix_windows_share_fallback_output_path(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> Option<(String, std::path::PathBuf)> {
        #[cfg(unix)]
        {
            let fallback_member_name =
                crate::path::make_unix_windows_share_member_name_compatible(member_name);
            if fallback_member_name == member_name {
                return None;
            }
            let member_path = std::path::Path::new(member_name);
            if member_name.is_empty() || !out_path.ends_with(member_path) {
                return None;
            }
            let fallback_path =
                Self::redirection_target_root(member_name, out_path).join(&fallback_member_name);
            if fallback_path == out_path {
                None
            } else {
                Some((fallback_member_name, fallback_path))
            }
        }

        #[cfg(not(unix))]
        {
            let _ = (member_name, out_path);
            None
        }
    }

    fn create_regular_output_file_at_member_path(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<std::fs::File> {
        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        Self::remove_existing_regular_output_symlink(out_path)?;
        Self::clear_windows_readonly_before_overwrite(out_path)?;
        std::fs::File::create(out_path).map_err(RarError::Io)
    }

    /// Best-effort physical preallocation of an output file. Reserves blocks
    /// only — the logical file size is untouched, so short writes on
    /// unknown-size members stay correct. Failures are ignored. Exposed so
    /// callers that own their output files (e.g. the server's extraction
    /// writers) can apply the same policy before streaming into them.
    pub fn preallocate_output_file(file: &std::fs::File, expected_len: u64) {
        const PREALLOCATE_MIN_BYTES: u64 = 8 * 1024 * 1024;
        if expected_len < PREALLOCATE_MIN_BYTES || expected_len > i64::MAX as u64 {
            return;
        }
        #[cfg(target_os = "macos")]
        {
            use std::os::fd::AsRawFd;
            let mut store = libc::fstore_t {
                fst_flags: libc::F_ALLOCATECONTIG,
                fst_posmode: libc::F_PEOFPOSMODE,
                fst_offset: 0,
                fst_length: expected_len as libc::off_t,
                fst_bytesalloc: 0,
            };
            // Contiguous first, then any-blocks fallback.
            let contiguous = unsafe {
                libc::fcntl(
                    file.as_raw_fd(),
                    libc::F_PREALLOCATE,
                    &mut store as *mut libc::fstore_t,
                )
            };
            if contiguous == -1 {
                store.fst_flags = libc::F_ALLOCATEALL;
                let _ = unsafe {
                    libc::fcntl(
                        file.as_raw_fd(),
                        libc::F_PREALLOCATE,
                        &mut store as *mut libc::fstore_t,
                    )
                };
            }
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let _ = unsafe {
                libc::fallocate(
                    file.as_raw_fd(),
                    libc::FALLOC_FL_KEEP_SIZE,
                    0,
                    expected_len as libc::off_t,
                )
            };
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = (file, expected_len);
        }
    }

    fn create_regular_output_file_for_member(
        member_name: &str,
        out_path: &std::path::Path,
    ) -> RarResult<(std::fs::File, std::path::PathBuf)> {
        match Self::create_regular_output_file_at_member_path(member_name, out_path) {
            Ok(file) => Ok((file, out_path.to_path_buf())),
            Err(original_err) => {
                if let Some((fallback_member_name, fallback_path)) =
                    Self::unix_windows_share_fallback_output_path(member_name, out_path)
                {
                    tracing::debug!(
                        original = %out_path.display(),
                        fallback = %fallback_path.display(),
                        "retrying file creation with UnRAR-compatible Windows-share name"
                    );
                    let file = Self::create_regular_output_file_at_member_path(
                        &fallback_member_name,
                        &fallback_path,
                    )?;
                    Ok((file, fallback_path))
                } else {
                    Err(original_err)
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_symlink_output(
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        options: &ExtractOptions,
        member_name: &str,
        raw_member_name: &str,
        target: &str,
        target_raw: Option<&[u8]>,
        windows_reparse_target: &str,
        target_is_directory: bool,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        Self::ensure_safe_symlink_target(fh, member_name, target, raw_member_name, out_path)?;
        Self::remove_existing_link_output(out_path)?;

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;

            let _ = windows_reparse_target;
            let _ = target_is_directory;
            let target = target_raw
                .map(std::ffi::OsStr::from_bytes)
                .unwrap_or_else(|| std::ffi::OsStr::new(target));
            std::os::unix::fs::symlink(target, out_path).map_err(RarError::Io)?;
            Self::apply_symlink_times(fh, out_path)?;
            if options.restore_owners {
                Self::apply_unix_owner(owner, out_path, false)?;
            }
            Ok(0)
        }

        #[cfg(windows)]
        {
            let _ = owner;
            let _ = options;
            let _ = target_raw;

            Self::ensure_safe_windows_reparse_target(windows_reparse_target, raw_member_name)?;
            Self::create_windows_symlink_reparse_point(
                windows_reparse_target,
                out_path,
                fh.is_directory || target_is_directory,
            )?;
            Self::apply_symlink_times(fh, out_path)?;
            Ok(0)
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = fh;
            let _ = owner;
            let _ = options;
            let _ = target;
            let _ = target_raw;
            let _ = windows_reparse_target;
            let _ = target_is_directory;
            let _ = out_path;
            Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "symlink".to_string(),
            })
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_windows_junction_output(
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        options: &ExtractOptions,
        member_name: &str,
        raw_member_name: &str,
        safety_target: &str,
        reparse_target: &str,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        Self::ensure_safe_symlink_target(
            fh,
            member_name,
            safety_target,
            raw_member_name,
            out_path,
        )?;
        Self::remove_existing_link_output(out_path)?;

        #[cfg(windows)]
        {
            let _ = owner;
            let _ = options;

            Self::create_windows_junction_reparse_point(reparse_target, out_path, raw_member_name)?;
            Self::apply_symlink_times(fh, out_path)?;
            Ok(0)
        }

        #[cfg(unix)]
        {
            let _ = reparse_target;
            let target = std::ffi::OsStr::new(safety_target);
            std::os::unix::fs::symlink(target, out_path).map_err(RarError::Io)?;
            Self::apply_symlink_times(fh, out_path)?;
            if options.restore_owners {
                Self::apply_unix_owner(owner, out_path, false)?;
            }
            Ok(0)
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = fh;
            let _ = owner;
            let _ = options;
            let _ = reparse_target;
            let _ = out_path;
            Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "WindowsJunction".to_string(),
            })
        }
    }

    #[cfg(windows)]
    fn create_windows_junction_reparse_point(
        target: &str,
        out_path: &std::path::Path,
        member: &str,
    ) -> RarResult<()> {
        Self::ensure_safe_windows_reparse_target(target, member)?;
        let buffer = Self::windows_junction_reparse_buffer(target)?;
        Self::create_windows_reparse_point(out_path, true, buffer)
    }

    #[cfg(windows)]
    fn create_windows_symlink_reparse_point(
        target: &str,
        out_path: &std::path::Path,
        is_directory: bool,
    ) -> RarResult<()> {
        let buffer = Self::windows_symlink_reparse_buffer(target)?;
        Self::create_windows_reparse_point(out_path, is_directory, buffer)
    }

    #[cfg(any(windows, test))]
    fn ensure_safe_windows_reparse_target(target: &str, member: &str) -> RarResult<()> {
        if Self::windows_reparse_target_is_absolute(target) {
            return Err(RarError::UnsafeLinkTarget {
                member: member.to_string(),
                target: target.to_string(),
            });
        }
        Ok(())
    }

    #[cfg(any(windows, test))]
    fn windows_reparse_target_is_absolute(target: &str) -> bool {
        target.starts_with('\\')
            || target.starts_with('/')
            || (target.len() >= 3
                && target.as_bytes()[0].is_ascii_alphabetic()
                && target.as_bytes()[1] == b':'
                && matches!(target.as_bytes()[2], b'\\' | b'/'))
    }

    #[cfg(windows)]
    fn create_windows_reparse_point(
        out_path: &std::path::Path,
        is_directory: bool,
        buffer: Vec<u8>,
    ) -> RarResult<()> {
        Self::ensure_windows_reparse_privileges();

        if is_directory {
            Self::create_windows_reparse_directory_placeholder(out_path)?;
        } else {
            Self::create_windows_reparse_file_placeholder(out_path)?;
        }

        let handle = Self::open_windows_file_with_long_path_fallback(
            out_path,
            windows_sys::Win32::Foundation::GENERIC_READ
                | windows_sys::Win32::Foundation::GENERIC_WRITE,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT
                | windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS,
        );
        if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            let err = std::io::Error::last_os_error();
            Self::remove_failed_windows_reparse_placeholder(out_path, is_directory);
            return Err(RarError::Io(err));
        }

        struct Handle(windows_sys::Win32::Foundation::HANDLE);
        impl Drop for Handle {
            fn drop(&mut self) {
                unsafe {
                    windows_sys::Win32::Foundation::CloseHandle(self.0);
                }
            }
        }
        let handle = Handle(handle);

        let mut returned = 0u32;
        let ok = unsafe {
            windows_sys::Win32::System::IO::DeviceIoControl(
                handle.0,
                windows_sys::Win32::System::Ioctl::FSCTL_SET_REPARSE_POINT,
                buffer.as_ptr().cast(),
                buffer.len() as u32,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            let err = std::io::Error::last_os_error();
            drop(handle);
            Self::remove_failed_windows_reparse_placeholder(out_path, is_directory);
            return Err(RarError::Io(err));
        }

        Ok(())
    }

    #[cfg(windows)]
    fn create_windows_reparse_directory_placeholder(out_path: &std::path::Path) -> RarResult<()> {
        let path = Self::windows_path_to_wide_nul(out_path);
        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::CreateDirectoryW(
                path.as_ptr(),
                std::ptr::null(),
            )
        };
        if ok != 0 {
            return Ok(());
        }

        let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
            return Err(RarError::Io(std::io::Error::last_os_error()));
        };
        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::CreateDirectoryW(
                long_path.as_ptr(),
                std::ptr::null(),
            )
        };
        if ok == 0 {
            return Err(RarError::Io(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    #[cfg(windows)]
    fn create_windows_reparse_file_placeholder(out_path: &std::path::Path) -> RarResult<()> {
        let handle = Self::create_windows_file_with_long_path_fallback(
            out_path,
            windows_sys::Win32::Foundation::GENERIC_WRITE,
            0,
            windows_sys::Win32::Storage::FileSystem::CREATE_NEW,
            windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_NORMAL,
        );
        if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            return Err(RarError::Io(std::io::Error::last_os_error()));
        }

        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(handle);
        }
        Ok(())
    }

    #[cfg(windows)]
    fn ensure_windows_reparse_privileges() {
        static ENABLED: OnceLock<()> = OnceLock::new();
        ENABLED.get_or_init(|| {
            // Mirrors UnRAR's best-effort SetPrivilege calls before creating
            // Windows symlink or junction reparse points. Missing privileges
            // are reported by the later FSCTL_SET_REPARSE_POINT call.
            Self::try_enable_windows_privilege(windows_sys::Win32::Security::SE_RESTORE_NAME);
            Self::try_enable_windows_privilege(
                windows_sys::Win32::Security::SE_CREATE_SYMBOLIC_LINK_NAME,
            );
        });
    }

    #[cfg(windows)]
    fn try_enable_windows_privilege(privilege_name: windows_sys::core::PCWSTR) {
        let mut token = windows_sys::Win32::Foundation::HANDLE::default();
        let opened = unsafe {
            windows_sys::Win32::System::Threading::OpenProcessToken(
                windows_sys::Win32::System::Threading::GetCurrentProcess(),
                windows_sys::Win32::Security::TOKEN_ADJUST_PRIVILEGES,
                &mut token,
            )
        };
        if opened == 0 {
            return;
        }

        struct TokenHandle(windows_sys::Win32::Foundation::HANDLE);
        impl Drop for TokenHandle {
            fn drop(&mut self) {
                unsafe {
                    windows_sys::Win32::Foundation::CloseHandle(self.0);
                }
            }
        }
        let token = TokenHandle(token);

        let mut privileges = windows_sys::Win32::Security::TOKEN_PRIVILEGES {
            PrivilegeCount: 1,
            Privileges: [windows_sys::Win32::Security::LUID_AND_ATTRIBUTES {
                Luid: windows_sys::Win32::Foundation::LUID {
                    LowPart: 0,
                    HighPart: 0,
                },
                Attributes: windows_sys::Win32::Security::SE_PRIVILEGE_ENABLED,
            }],
        };

        let looked_up = unsafe {
            windows_sys::Win32::Security::LookupPrivilegeValueW(
                std::ptr::null(),
                privilege_name,
                &mut privileges.Privileges[0].Luid,
            )
        };
        if looked_up == 0 {
            return;
        }

        unsafe {
            windows_sys::Win32::Security::AdjustTokenPrivileges(
                token.0,
                0,
                &privileges,
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }
    }

    #[cfg(windows)]
    fn remove_failed_windows_reparse_placeholder(out_path: &std::path::Path, is_directory: bool) {
        let _ = if is_directory {
            Self::remove_output_dir(out_path)
        } else {
            Self::remove_output_file(out_path)
        };
    }

    #[cfg(any(windows, test))]
    fn windows_junction_reparse_buffer(target: &str) -> RarResult<Vec<u8>> {
        let substitute_name = Self::windows_junction_substitute_name(target);
        let print_name = Self::windows_reparse_print_name(&substitute_name);
        let substitute = Self::windows_string_to_wide_nul(&substitute_name);
        let print = Self::windows_string_to_wide_nul(&print_name);

        let substitute_len = (substitute.len().saturating_sub(1))
            .checked_mul(2)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows junction target is too long".into(),
            })?;
        let print_offset = substitute
            .len()
            .checked_mul(2)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows junction target is too long".into(),
            })?;
        let print_len = (print.len().saturating_sub(1))
            .checked_mul(2)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows junction target is too long".into(),
            })?;

        let path_bytes = substitute
            .len()
            .checked_add(print.len())
            .and_then(|value| value.checked_mul(2))
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows junction target is too long".into(),
            })?;
        let reparse_data_len = u16::try_from(8usize.checked_add(path_bytes).ok_or_else(|| {
            RarError::CorruptArchive {
                detail: "RAR5: Windows junction target is too long".into(),
            }
        })?)
        .map_err(|_| RarError::CorruptArchive {
            detail: "RAR5: Windows junction target is too long".into(),
        })?;

        let mut buffer = Vec::with_capacity(8 + usize::from(reparse_data_len));
        buffer.extend_from_slice(&WINDOWS_IO_REPARSE_TAG_MOUNT_POINT.to_le_bytes());
        buffer.extend_from_slice(&reparse_data_len.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.extend_from_slice(&substitute_len.to_le_bytes());
        buffer.extend_from_slice(&print_offset.to_le_bytes());
        buffer.extend_from_slice(&print_len.to_le_bytes());
        for unit in substitute.iter().chain(print.iter()) {
            buffer.extend_from_slice(&unit.to_le_bytes());
        }
        Ok(buffer)
    }

    #[cfg(any(windows, test))]
    fn windows_symlink_reparse_buffer(target: &str) -> RarResult<Vec<u8>> {
        let substitute_name = target.replace('/', "\\");
        let print_name = Self::windows_reparse_print_name(&substitute_name);
        let substitute = Self::windows_string_to_wide_nul(&substitute_name);
        let print = Self::windows_string_to_wide_nul(&print_name);

        let substitute_len = (substitute.len().saturating_sub(1))
            .checked_mul(2)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows symlink target is too long".into(),
            })?;
        let print_offset = substitute
            .len()
            .checked_mul(2)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows symlink target is too long".into(),
            })?;
        let print_len = (print.len().saturating_sub(1))
            .checked_mul(2)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows symlink target is too long".into(),
            })?;

        let path_bytes = substitute
            .len()
            .checked_add(print.len())
            .and_then(|value| value.checked_mul(2))
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "RAR5: Windows symlink target is too long".into(),
            })?;
        let reparse_data_len = u16::try_from(12usize.checked_add(path_bytes).ok_or_else(|| {
            RarError::CorruptArchive {
                detail: "RAR5: Windows symlink target is too long".into(),
            }
        })?)
        .map_err(|_| RarError::CorruptArchive {
            detail: "RAR5: Windows symlink target is too long".into(),
        })?;

        let flags = if Self::windows_reparse_target_is_absolute(&substitute_name) {
            0
        } else {
            WINDOWS_SYMLINK_FLAG_RELATIVE
        };

        let mut buffer = Vec::with_capacity(8 + usize::from(reparse_data_len));
        buffer.extend_from_slice(&WINDOWS_IO_REPARSE_TAG_SYMLINK.to_le_bytes());
        buffer.extend_from_slice(&reparse_data_len.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.extend_from_slice(&0u16.to_le_bytes());
        buffer.extend_from_slice(&substitute_len.to_le_bytes());
        buffer.extend_from_slice(&print_offset.to_le_bytes());
        buffer.extend_from_slice(&print_len.to_le_bytes());
        buffer.extend_from_slice(&flags.to_le_bytes());
        for unit in substitute.iter().chain(print.iter()) {
            buffer.extend_from_slice(&unit.to_le_bytes());
        }
        Ok(buffer)
    }

    #[cfg(any(windows, test))]
    fn windows_junction_substitute_name(target: &str) -> String {
        if let Some(rest) = target.strip_prefix("/??/") {
            format!("\\??\\{}", rest.replace('/', "\\"))
        } else {
            target.replace('/', "\\")
        }
    }

    #[cfg(test)]
    fn windows_junction_print_name(substitute_name: &str) -> String {
        Self::windows_reparse_print_name(substitute_name)
    }

    #[cfg(any(windows, test))]
    fn windows_reparse_print_name(substitute_name: &str) -> String {
        let print = substitute_name
            .strip_prefix("\\??\\")
            .unwrap_or(substitute_name);
        if let Some(rest) = print.strip_prefix("UNC\\") {
            format!("\\{rest}")
        } else {
            print.to_string()
        }
    }

    #[cfg(any(windows, test))]
    fn windows_string_to_wide_nul(value: &str) -> Vec<u16> {
        value.encode_utf16().chain(std::iter::once(0)).collect()
    }

    #[cfg(windows)]
    fn windows_path_to_wide_nul(path: &std::path::Path) -> Vec<u16> {
        use std::os::windows::ffi::OsStrExt;

        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    #[cfg(windows)]
    fn open_windows_file_with_long_path_fallback(
        out_path: &std::path::Path,
        desired_access: u32,
        share_mode: u32,
        flags_and_attributes: u32,
    ) -> windows_sys::Win32::Foundation::HANDLE {
        Self::create_windows_file_with_long_path_fallback(
            out_path,
            desired_access,
            share_mode,
            windows_sys::Win32::Storage::FileSystem::OPEN_EXISTING,
            flags_and_attributes,
        )
    }

    #[cfg(windows)]
    fn create_windows_file_with_long_path_fallback(
        out_path: &std::path::Path,
        desired_access: u32,
        share_mode: u32,
        creation_disposition: u32,
        flags_and_attributes: u32,
    ) -> windows_sys::Win32::Foundation::HANDLE {
        let path = Self::windows_path_to_wide_nul(out_path);
        let handle = unsafe {
            windows_sys::Win32::Storage::FileSystem::CreateFileW(
                path.as_ptr(),
                desired_access,
                share_mode,
                std::ptr::null(),
                creation_disposition,
                flags_and_attributes,
                std::ptr::null_mut(),
            )
        };
        if handle != windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            return handle;
        }

        let Some(long_path) = Self::windows_long_path_to_wide_nul(out_path) else {
            return handle;
        };
        unsafe {
            windows_sys::Win32::Storage::FileSystem::CreateFileW(
                long_path.as_ptr(),
                desired_access,
                share_mode,
                std::ptr::null(),
                creation_disposition,
                flags_and_attributes,
                std::ptr::null_mut(),
            )
        }
    }

    fn create_rar3_unix_symlink_output(
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        options: &ExtractOptions,
        member_name: &str,
        raw_member_name: &str,
        target: &Rar3UnixLinkTarget,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
        Self::ensure_output_parent_dir_for_member(member_name, out_path)?;
        Self::ensure_safe_symlink_target(
            fh,
            member_name,
            &target.safety_target,
            raw_member_name,
            out_path,
        )?;
        Self::remove_existing_link_output(out_path)?;

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;

            let raw_target = std::ffi::OsStr::from_bytes(&target.raw);
            std::os::unix::fs::symlink(raw_target, out_path).map_err(RarError::Io)?;
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
            let _ = member_name;
            let _ = out_path;
            Err(RarError::UnsupportedLinkType {
                member: raw_member_name.to_string(),
                link_type: "RAR3 Unix symlink".to_string(),
            })
        }
    }

    #[cfg(unix)]
    fn apply_symlink_times(fh: &FileHeader, out_path: &std::path::Path) -> RarResult<()> {
        if let Some((atime, mtime)) =
            Self::unix_regular_file_times(fh.mtime, fh.atime, std::time::SystemTime::now())
        {
            // Scryer's Unix support is Darwin and Linux; filetime maps both to
            // the platform no-follow symlink timestamp APIs.
            if let Err(error) = filetime::set_symlink_file_times(out_path, atime, mtime) {
                debug!(
                    path = %out_path.display(),
                    ?error,
                    "failed to restore symlink timestamps"
                );
            }
        }
        Ok(())
    }

    #[cfg(windows)]
    fn apply_symlink_times(fh: &FileHeader, out_path: &std::path::Path) -> RarResult<()> {
        let has_mtime = fh.mtime.is_some();
        let has_atime = fh.atime.is_some();
        let has_ctime = fh.ctime.is_some();
        if !has_mtime && !has_atime && !has_ctime {
            return Ok(());
        }

        let to_filetime = |time: std::time::SystemTime| -> RarResult<_> {
            let intervals = Self::windows_filetime_intervals(time)?;
            Ok(windows_sys::Win32::Foundation::FILETIME {
                dwLowDateTime: intervals as u32,
                dwHighDateTime: (intervals >> 32) as u32,
            })
        };
        let ctime = fh.ctime.map(to_filetime).transpose()?;
        let atime = fh.atime.map(to_filetime).transpose()?;
        let mtime = fh.mtime.map(to_filetime).transpose()?;

        let handle = Self::open_windows_file_with_long_path_fallback(
            out_path,
            windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT
                | windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS,
        );
        if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            let error = std::io::Error::last_os_error();
            debug!(
                path = %out_path.display(),
                ?error,
                "failed to open symlink for timestamp restore"
            );
            return Ok(());
        }

        struct Handle(windows_sys::Win32::Foundation::HANDLE);
        impl Drop for Handle {
            fn drop(&mut self) {
                unsafe {
                    windows_sys::Win32::Foundation::CloseHandle(self.0);
                }
            }
        }
        let handle = Handle(handle);

        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::SetFileTime(
                handle.0,
                ctime
                    .as_ref()
                    .map_or(std::ptr::null(), |value| value as *const _),
                atime
                    .as_ref()
                    .map_or(std::ptr::null(), |value| value as *const _),
                mtime
                    .as_ref()
                    .map_or(std::ptr::null(), |value| value as *const _),
            )
        };
        if ok == 0 {
            let error = std::io::Error::last_os_error();
            debug!(
                path = %out_path.display(),
                ?error,
                "failed to restore symlink timestamps"
            );
        }
        Ok(())
    }

    fn apply_output_times(fh: &FileHeader, out_path: &std::path::Path) -> RarResult<()> {
        #[cfg(unix)]
        {
            if let Some((atime, mtime)) =
                Self::unix_regular_file_times(fh.mtime, fh.atime, std::time::SystemTime::now())
                && let Err(error) = filetime::set_file_times(out_path, atime, mtime)
            {
                debug!(
                    path = %out_path.display(),
                    ?error,
                    "failed to restore output timestamps"
                );
            }
        }

        #[cfg(windows)]
        {
            let result = match (fh.mtime, fh.atime) {
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
            };
            if let Err(error) = result {
                debug!(
                    path = %out_path.display(),
                    ?error,
                    "failed to restore output timestamps"
                );
            }
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = fh;
            let _ = out_path;
        }

        Self::apply_creation_time(fh.ctime, out_path)
    }

    #[cfg(any(unix, test))]
    fn unix_regular_file_times(
        mtime: Option<std::time::SystemTime>,
        atime: Option<std::time::SystemTime>,
        now: std::time::SystemTime,
    ) -> Option<(filetime::FileTime, filetime::FileTime)> {
        if mtime.is_none() && atime.is_none() {
            return None;
        }

        let atime = atime.unwrap_or(now);
        let mtime = mtime.unwrap_or(now);
        Some((
            filetime::FileTime::from_system_time(atime),
            filetime::FileTime::from_system_time(mtime),
        ))
    }

    #[cfg(windows)]
    fn apply_creation_time(
        ctime: Option<std::time::SystemTime>,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        let Some(ctime) = ctime else {
            return Ok(());
        };
        let intervals = Self::windows_filetime_intervals(ctime)?;
        let creation = windows_sys::Win32::Foundation::FILETIME {
            dwLowDateTime: intervals as u32,
            dwHighDateTime: (intervals >> 32) as u32,
        };

        let handle = Self::open_windows_file_with_long_path_fallback(
            out_path,
            windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS,
        );
        if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            let error = std::io::Error::last_os_error();
            debug!(
                path = %out_path.display(),
                ?error,
                "failed to open output for creation timestamp restore"
            );
            return Ok(());
        }

        struct Handle(windows_sys::Win32::Foundation::HANDLE);
        impl Drop for Handle {
            fn drop(&mut self) {
                unsafe {
                    windows_sys::Win32::Foundation::CloseHandle(self.0);
                }
            }
        }
        let handle = Handle(handle);

        let ok = unsafe {
            windows_sys::Win32::Storage::FileSystem::SetFileTime(
                handle.0,
                &creation,
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        if ok == 0 {
            let error = std::io::Error::last_os_error();
            debug!(
                path = %out_path.display(),
                ?error,
                "failed to restore output creation timestamp"
            );
        }
        Ok(())
    }

    #[cfg(not(windows))]
    fn apply_creation_time(
        ctime: Option<std::time::SystemTime>,
        out_path: &std::path::Path,
    ) -> RarResult<()> {
        let _ = ctime;
        let _ = out_path;
        Ok(())
    }

    #[cfg(any(windows, test))]
    fn windows_filetime_intervals(time: std::time::SystemTime) -> RarResult<u64> {
        const WINDOWS_TO_UNIX_EPOCH_SECS: u64 = 11_644_473_600;
        let windows_epoch =
            std::time::UNIX_EPOCH - std::time::Duration::from_secs(WINDOWS_TO_UNIX_EPOCH_SECS);
        let duration =
            time.duration_since(windows_epoch)
                .map_err(|_| RarError::CorruptArchive {
                    detail: "RAR timestamp predates Windows FILETIME epoch".into(),
                })?;
        duration
            .as_secs()
            .checked_mul(10_000_000)
            .and_then(|value| value.checked_add(u64::from(duration.subsec_nanos() / 100)))
            .ok_or_else(|| RarError::ResourceLimit {
                detail: "RAR timestamp overflows Windows FILETIME".into(),
            })
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

    #[cfg(any(windows, test))]
    fn windows_output_attributes(fh: &FileHeader) -> u32 {
        match fh.host_os {
            crate::types::HostOs::Windows => fh.attributes.0 as u32,
            // Scryer only maps Windows, Unix/Linux, and Darwin behavior. On
            // Windows, UnRAR converts non-Windows archive origins to normal
            // Win32 directory/file attributes instead of treating their mode
            // bits as native Windows flags.
            crate::types::HostOs::Unix
            | crate::types::HostOs::Darwin
            | crate::types::HostOs::Unknown(_) => {
                if fh.is_directory {
                    WINDOWS_FILE_ATTRIBUTE_DIRECTORY
                } else {
                    WINDOWS_FILE_ATTRIBUTE_ARCHIVE
                }
            }
        }
    }

    fn apply_windows_attributes(fh: &FileHeader, out_path: &std::path::Path) -> RarResult<()> {
        #[cfg(windows)]
        {
            let attributes = Self::windows_output_attributes(fh);
            Self::set_windows_file_attributes(out_path, attributes)?;
        }

        #[cfg(not(windows))]
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
            let chown_follows_links = Self::unix_owner_chown_follows_links(follow_links);
            match Self::path_owner_matches(out_path, uid, gid, chown_follows_links) {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error) => {
                    debug!(
                        path = %out_path.display(),
                        error = %error,
                        "failed to inspect Unix owner before restore"
                    );
                }
            }
            let original_mode = Self::unix_followed_mode(out_path);

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
                if chown_follows_links {
                    libc::chown(path.as_ptr(), uid_arg, gid_arg)
                } else {
                    libc::lchown(path.as_ptr(), uid_arg, gid_arg)
                }
            };
            if result != 0 {
                debug!(
                    path = %out_path.display(),
                    error = %std::io::Error::last_os_error(),
                    "failed to restore Unix owner"
                );
                return Ok(());
            }
            if let Some(mode) = original_mode
                && let Err(error) = Self::restore_unix_followed_mode(out_path, mode)
            {
                debug!(
                    path = %out_path.display(),
                    error = %error,
                    "failed to restore Unix mode after owner restore"
                );
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

    #[cfg(any(unix, test))]
    fn unix_owner_chown_follows_links(follow_links: bool) -> bool {
        follow_links || cfg!(target_vendor = "apple")
    }

    #[cfg(any(unix, test))]
    fn unix_followed_mode(out_path: &std::path::Path) -> Option<u32> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::metadata(out_path)
                .ok()
                .map(|metadata| metadata.permissions().mode())
        }

        #[cfg(not(unix))]
        {
            let _ = out_path;
            None
        }
    }

    #[cfg(any(unix, test))]
    fn restore_unix_followed_mode(out_path: &std::path::Path, mode: u32) -> RarResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = std::fs::metadata(out_path)
                .map_err(RarError::Io)?
                .permissions();
            permissions.set_mode(mode);
            std::fs::set_permissions(out_path, permissions).map_err(RarError::Io)
        }

        #[cfg(not(unix))]
        {
            let _ = out_path;
            let _ = mode;
            Ok(())
        }
    }

    #[cfg(unix)]
    fn resolve_unix_owner(
        owner: &crate::types::UnixOwnerInfo,
    ) -> RarResult<Option<(Option<libc::uid_t>, Option<libc::gid_t>)>> {
        let mut uid = owner.uid.map(Self::uid_from_archive).transpose()?;
        let mut gid = owner.gid.map(Self::gid_from_archive).transpose()?;

        if let Some(name) = owner.user_name_bytes().filter(|name| !name.is_empty()) {
            match Self::lookup_uid_by_name(name)? {
                Some(resolved) => uid = Some(resolved),
                None if uid.is_none() => return Ok(None),
                None => {}
            }
        }

        if let Some(name) = owner.group_name_bytes().filter(|name| !name.is_empty()) {
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
    fn lookup_uid_by_name(name: &[u8]) -> RarResult<Option<libc::uid_t>> {
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
    fn lookup_gid_by_name(name: &[u8]) -> RarResult<Option<libc::gid_t>> {
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

    #[cfg(unix)]
    fn unix_output_mode(fh: &FileHeader) -> Option<u32> {
        match fh.host_os {
            crate::types::HostOs::Unix => {
                let mode = fh.attributes.unix_mode() & 0o7777;
                Some(mode)
            }
            crate::types::HostOs::Darwin if !fh.compression.format.is_rar4_family() => {
                let mode = fh.attributes.unix_mode() & 0o7777;
                Some(mode)
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
            crate::types::HostOs::Darwin | crate::types::HostOs::Unknown(_) => {
                // RAR4 `HSYS_MACOS` is a legacy Mac marker, not POSIX mode
                // storage. Official UnRAR's Unix `ConvertAttributes` treats
                // it like other non-Unix legacy hosts, so only real Unix
                // archive origins preserve raw mode bits here.
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
        Self::apply_unix_permissions(fh, out_path)?;
        Self::apply_windows_attributes(fh, out_path)
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
        // UnRAR sets attributes for hardlinks after creation because deleting
        // or replacing the output link can affect the shared target metadata.
        Self::apply_unix_permissions(fh, out_path)?;
        Self::apply_windows_attributes(fh, out_path)?;
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

    #[allow(clippy::too_many_arguments)]
    fn create_filecopy_output(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        fh: &FileHeader,
        owner: Option<&crate::types::UnixOwnerInfo>,
        member_name: &str,
        raw_member_name: &str,
        target: &str,
        out_path: &std::path::Path,
    ) -> RarResult<u64> {
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

        let target_member_name = crate::path::sanitize_file_redirection_path(target);
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
                crate::path::sanitize_member_path(
                    self.format,
                    entry.file_header.host_os,
                    &entry.file_header.name,
                ) == target_member_name
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
        if source_index > index {
            return Err(RarError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "RAR file-copy source member {target} appears after {raw_member_name} and is not available yet"
                ),
            )));
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
            let _ = Self::remove_output_file(&temp_path);
            return Err(error);
        }

        Self::remove_existing_link_output(out_path)?;
        let copy_result = Self::copy_existing_filecopy_source(&temp_path, out_path);
        let remove_result = Self::remove_output_file(&temp_path);
        match (copy_result, remove_result) {
            (Ok(written), Ok(())) => {
                Self::apply_output_metadata(fh, owner, options, out_path)?;
                Ok(written)
            }
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
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
        let member_name = crate::path::sanitize_member_path(self.format, fh.host_os, &fh.name);

        if let Some(redir) = redirection {
            let link_type = format!("{:?}", redir.redir_type);
            let target = Self::normalized_rar5_redirection_target(redir.redir_type, &redir.target);
            let target_raw = if matches!(redir.redir_type, header::RedirectionType::UnixSymlink) {
                redir.target_raw.as_deref()
            } else {
                None
            };
            return match redir.redir_type {
                header::RedirectionType::UnixSymlink | header::RedirectionType::WindowsSymlink => {
                    Self::create_symlink_output(
                        fh,
                        owner,
                        options,
                        &member_name,
                        &fh.name,
                        &target,
                        target_raw,
                        &redir.target,
                        redir.target_is_directory,
                        out_path,
                    )
                    .map(Some)
                }
                header::RedirectionType::WindowsJunction => Self::create_windows_junction_output(
                    fh,
                    owner,
                    options,
                    &member_name,
                    &fh.name,
                    &target,
                    &redir.target,
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
            return Self::create_rar3_unix_symlink_output(
                fh,
                owner,
                options,
                &member_name,
                &fh.name,
                &target,
                out_path,
            )
            .map(Some);
        }

        Ok(None)
    }

    fn unsupported_non_file_link_type(
        &self,
        entry: &MemberEntry,
        fh: &FileHeader,
    ) -> Option<String> {
        if let Some(redirection) = &entry.redirection {
            return Some(format!("{:?}", redirection.redir_type));
        }

        let rar3_unix_symlink = self.format.is_rar4_family()
            && matches!(fh.host_os, crate::types::HostOs::Unix)
            && (fh.attributes.0 & 0xF000) == 0xA000;
        rar3_unix_symlink.then(|| "RAR3 Unix symlink".to_string())
    }

    fn reject_link_member_without_file_target(
        &self,
        entry: &MemberEntry,
        fh: &FileHeader,
    ) -> RarResult<()> {
        if let Some(link_type) = self.unsupported_non_file_link_type(entry, fh) {
            return Err(RarError::UnsupportedLinkType {
                member: fh.name.clone(),
                link_type: format!("{link_type} requires extract_member_to_file"),
            });
        }

        Ok(())
    }

    fn advance_solid_cursor_to(
        &mut self,
        index: usize,
        fh: &FileHeader,
        password: Option<&str>,
    ) -> RarResult<()> {
        let effective_password = password
            .map(str::to_owned)
            .or_else(|| self.password.clone());

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
                        effective_password
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
                )
                .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));

                if skip_entry.is_encrypted {
                    let password = effective_password
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
        password: Option<&str>,
    ) -> RarResult<()> {
        let effective_password = password
            .map(str::to_owned)
            .or_else(|| self.password.clone());

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
                    .with_member_name(&skip_fh.name)
                    .with_max_data_segment(self.limits.max_data_segment)
                    .with_continuation(skip_fh.split_after, self.format, effective_password.clone())
                    .with_kdf_cache(Arc::clone(&self.kdf_cache));

                if skip_entry.is_encrypted {
                    let password = effective_password
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
                        let base_reader = base_reader.with_packed_hash_key(Some(crypto.hash_key));
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
        let read = reader.read(&mut probe).map_err(Self::read_error_to_rar)?;
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
            legacy
                if fh.compression.format == ArchiveFormat::Rar4
                    && matches!(fh.host_os, crate::types::HostOs::Unknown(0)) =>
            {
                crate::crypto::DecryptingReader::new_rar4_legacy_dos(reader, legacy, password)
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

    fn read_error_to_rar(error: std::io::Error) -> RarError {
        let kind = error.kind();
        let message = error.to_string();
        match error.into_inner() {
            Some(inner) => match inner.downcast::<RarError>() {
                Ok(error) => *error,
                Err(inner) => RarError::Io(std::io::Error::new(kind, inner)),
            },
            None => RarError::Io(std::io::Error::new(kind, message)),
        }
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

            let read = reader
                .read(&mut buffer[..to_read])
                .map_err(Self::read_error_to_rar)?;
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

            let read = reader
                .read(&mut buffer[..to_read])
                .map_err(Self::read_error_to_rar)?;
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
        self.extract_member_with_link_policy(index, options, progress, false)
    }

    fn extract_member_with_link_policy(
        &mut self,
        index: usize,
        options: &ExtractOptions,
        progress: Option<&dyn ProgressHandler>,
        allow_link_payload: bool,
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
        if !allow_link_payload {
            self.reject_link_member_without_file_target(entry, &fh)?;
        }
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
            // Hashing runs off-thread for large members (see hash_pipeline).
            let stream_hash = SharedHashStream::new(
                matches!(tracked_data_hash, Some(crate::types::DataHash::Crc32(_))),
                matches!(tracked_data_hash, Some(crate::types::DataHash::Rar14(_))),
                expected_blake.is_some(),
                self.target_unpacked_size(&fh),
            );
            let (written, actual_crc, actual_data_hash, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                        .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
                let mut hash_writer = HashTrackingWriter {
                    inner: &mut output,
                    hash: Some(stream_hash.clone()),
                };

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

                let outputs = stream_hash.finalize().map_err(RarError::Io)?;
                (
                    written,
                    expected_crc.map(|_| outputs.crc32.unwrap_or(0)),
                    expected_data_hash.map(|expected| match expected {
                        crate::types::DataHash::Crc32(_) => {
                            crate::types::DataHash::Crc32(outputs.crc32.unwrap_or(0))
                        }
                        crate::types::DataHash::Rar14(_) => {
                            crate::types::DataHash::Rar14(outputs.rar14.unwrap_or(0))
                        }
                    }),
                    expected_blake.map(|_| outputs.blake2sp.unwrap_or([0; 32])),
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
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                        .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
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
            // Hashing runs off-thread for large members (see hash_pipeline).
            let stream_hash = SharedHashStream::new(
                matches!(tracked_data_hash, Some(crate::types::DataHash::Crc32(_))),
                matches!(tracked_data_hash, Some(crate::types::DataHash::Rar14(_))),
                expected_blake.is_some(),
                self.target_unpacked_size(&fh),
            );
            let (actual_crc, actual_data_hash, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                        .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
                let mut hash_writer = HashTrackingWriter {
                    inner: &mut output,
                    hash: Some(stream_hash.clone()),
                };
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
                let outputs = stream_hash.finalize().map_err(RarError::Io)?;
                (
                    expected_crc.map(|_| outputs.crc32.unwrap_or(0)),
                    expected_data_hash.map(|expected| match expected {
                        crate::types::DataHash::Crc32(_) => {
                            crate::types::DataHash::Crc32(outputs.crc32.unwrap_or(0))
                        }
                        crate::types::DataHash::Rar14(_) => {
                            crate::types::DataHash::Rar14(outputs.rar14.unwrap_or(0))
                        }
                    }),
                    expected_blake.map(|_| outputs.blake2sp.unwrap_or([0; 32])),
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
                self.advance_solid_cursor_to(index, &fh, options.password.as_deref())?;
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                        .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
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
        let member_name = crate::path::sanitize_member_path(self.format, fh.host_os, &fh.name);
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
            let resolved_out_path =
                Self::ensure_output_directory_for_member(&member_name, out_path)?;
            Self::apply_output_metadata(&fh, owner.as_ref(), options, &resolved_out_path)?;
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
        let (file, resolved_out_path) =
            Self::create_regular_output_file_for_member(&member_name, out_path)?;
        let out_path = resolved_out_path.as_path();
        Self::preallocate_output_file(&file, self.target_unpacked_size(&fh));
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
            // Hashing runs off-thread for large members (see hash_pipeline).
            let stream_hash = SharedHashStream::new(
                matches!(tracked_data_hash, Some(crate::types::DataHash::Crc32(_))),
                matches!(tracked_data_hash, Some(crate::types::DataHash::Rar14(_))),
                expected_blake.is_some(),
                self.target_unpacked_size(&fh),
            );
            let (written, actual_crc, actual_data_hash, actual_blake) = {
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                        .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
                let mut hash_writer = HashTrackingWriter {
                    inner: &mut writer,
                    hash: Some(stream_hash.clone()),
                };

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

                let outputs = stream_hash.finalize().map_err(RarError::Io)?;
                (
                    written,
                    expected_crc.map(|_| outputs.crc32.unwrap_or(0)),
                    expected_data_hash.map(|expected| match expected {
                        crate::types::DataHash::Crc32(_) => {
                            crate::types::DataHash::Crc32(outputs.crc32.unwrap_or(0))
                        }
                        crate::types::DataHash::Rar14(_) => {
                            crate::types::DataHash::Rar14(outputs.rar14.unwrap_or(0))
                        }
                    }),
                    expected_blake.map(|_| outputs.blake2sp.unwrap_or([0; 32])),
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
            self.restore_ntfs_acl_for_member(index, options, out_path)?;
            self.restore_ntfs_streams_for_member(index, options, out_path)?;
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
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                    .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));

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
            self.restore_ntfs_acl_for_member(index, options, out_path)?;
            self.restore_ntfs_streams_for_member(index, options, out_path)?;
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
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                    .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));

            let tracked_data_hash =
                expected_data_hash.or(expected_crc.map(crate::types::DataHash::Crc32));
            // Hashing runs off-thread for large members (see hash_pipeline).
            let stream_hash = SharedHashStream::new(
                matches!(tracked_data_hash, Some(crate::types::DataHash::Crc32(_))),
                matches!(tracked_data_hash, Some(crate::types::DataHash::Rar14(_))),
                expected_blake.is_some(),
                unpacked_size,
            );
            let (written, actual_crc, actual_data_hash, actual_blake) = {
                let mut hash_writer = HashTrackingWriter {
                    inner: &mut writer,
                    hash: Some(stream_hash.clone()),
                };
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
                let outputs = stream_hash.finalize().map_err(RarError::Io)?;
                let actual_crc = expected_crc.map(|_| outputs.crc32.unwrap_or(0));
                let actual_data_hash = expected_data_hash.map(|expected| match expected {
                    crate::types::DataHash::Crc32(_) => {
                        crate::types::DataHash::Crc32(outputs.crc32.unwrap_or(0))
                    }
                    crate::types::DataHash::Rar14(_) => {
                        crate::types::DataHash::Rar14(outputs.rar14.unwrap_or(0))
                    }
                });
                let actual_blake = expected_blake.map(|_| outputs.blake2sp.unwrap_or([0; 32]));
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
            self.restore_ntfs_acl_for_member(index, options, out_path)?;
            self.restore_ntfs_streams_for_member(index, options, out_path)?;
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
                self.advance_solid_cursor_to(index, &fh, options.password.as_deref())?;
                let segments = self.members[index].segments.clone();
                let base_reader =
                    ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                        .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key));
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
            self.restore_ntfs_acl_for_member(index, options, out_path)?;
            self.restore_ntfs_streams_for_member(index, options, out_path)?;
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
            let shared_hash = (expected_crc.is_some() || expected_blake.is_some()).then(|| {
                SharedHashStream::new(
                    expected_crc.is_some(),
                    false,
                    expected_blake.is_some(),
                    unpacked_size,
                )
            });
            self.advance_solid_cursor_to(index, &fh, options.password.as_deref())?;

            let volume_tracker = Arc::new(AtomicUsize::new(
                segments.first().map_or(0, |segment| segment.volume_index),
            ));
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let base_reader =
                ArchiveSegmentReader::new(&mut self.volumes, &self.limits, &segments, &fh.name)
                    .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
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
                        shared_hash.clone(),
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
                        shared_hash.clone(),
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
                    shared_hash.clone(),
                )?;
                self.enforce_unknown_lz_chunk_limit(&fh, &chunks)?;
                chunks
            };

            let (actual_crc, actual_blake) = finalize_shared_hash(shared_hash)?;
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
                .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
                .with_volume_tracker(Arc::clone(&volume_tracker));

        let shared_hash = (expected_crc.is_some() || expected_blake.is_some()).then(|| {
            SharedHashStream::new(
                expected_crc.is_some(),
                false,
                expected_blake.is_some(),
                unpacked_size,
            )
        });
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
                            hash: shared_hash.clone(),
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
                            hash: shared_hash.clone(),
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
                        hash: shared_hash.clone(),
                    }))
                },
                store_limit,
                &fh.name,
            )?
        };

        let (actual_crc, actual_blake) = finalize_shared_hash(shared_hash)?;
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
                decoder.ensure_solid_member_compat(dict_size, fh.compression.version)?;
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
        shared_hash: Option<Arc<SharedHashStream>>,
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
            let hash_clone = shared_hash.clone();
            decoder.decompress_reader_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                Arc::clone(&shared_transitions),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if hash_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            hash: hash_clone.clone(),
                        }))
                    } else {
                        Ok(writer)
                    }
                },
            )?
        } else {
            let decoder = if let Some(decoder) = solid_decoder {
                decoder.ensure_solid_member_compat(dict_size, fh.compression.version)?;
                decoder.prepare_solid_continuation();
                decoder
            } else {
                solid_decoder.insert(LzDecoder::try_new(dict_size, fh.compression.version)?)
            };
            let hash_clone = shared_hash.clone();
            decoder.decompress_reader_to_writer_chunked(
                compressed,
                unpacked_size,
                first_volume_index,
                Arc::clone(&shared_transitions),
                |volume_index| {
                    let writer = writer_factory(volume_index)?;
                    if hash_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            hash: hash_clone.clone(),
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
        self.reject_link_member_without_file_target(entry, &fh)?;
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
        let compute_crc = expected_crc.is_some() || (options.verify && split_after);
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);

        self.advance_solid_cursor_to_streaming(index, fh, provider, options.password.as_deref())?;

        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(&segments, provider)
            .with_member_name(&fh.name)
            .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(
                split_after,
                self.format,
                password
                    .map(str::to_owned)
                    .or_else(|| self.password.clone()),
            )
            .with_kdf_cache(Arc::clone(&self.kdf_cache))
            .with_metadata_sink(Rc::clone(&cont_meta));
        let unpacked_size = self.target_unpacked_size(fh);

        // Hashing runs off-thread for large members (see hash_pipeline).
        let stream_hash = (compute_crc || compute_blake2)
            .then(|| SharedHashStream::new(compute_crc, false, compute_blake2, unpacked_size));
        let (written, actual_crc, actual_blake) = {
            let mut hash_writer = HashTrackingWriter {
                inner: writer,
                hash: stream_hash.clone(),
            };

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

            let (actual_crc, actual_blake) = finalize_shared_hash(stream_hash)?;
            (written, actual_crc, actual_blake)
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
        let rar5_crypto = if self.format == ArchiveFormat::Rar5 {
            password
                .map(|pwd| self.prepare_rar5_encrypted_member(&fh.name, pwd, file_encryption))
                .transpose()?
        } else {
            None
        };
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(segments, provider)
            .with_member_name(&fh.name)
            .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(
                split_after,
                self.format,
                password
                    .map(str::to_owned)
                    .or_else(|| self.password.clone()),
            )
            .with_kdf_cache(Arc::clone(&self.kdf_cache))
            .with_metadata_sink(Rc::clone(&cont_meta));
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
        let compute_crc = expected_crc.is_some() || (options.verify && split_after);
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);

        // Wrap in DecryptingReader if encrypted, otherwise read directly.
        // Hashing runs off-thread for large members (see hash_pipeline).
        let stream_hash = SharedHashStream::new(compute_crc, false, compute_blake2, _unpacked_size);
        let mut written = 0u64;

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
            let to_read = store_limit.next_read_len(written, STREAMING_STORE_CHUNK_BUFFER_BYTES);
            if to_read == 0 {
                Self::enforce_store_ceiling_not_exceeded(&mut reader, store_limit, &fh.name)?;
                break;
            }
            let mut chunk = stream_hash.take_buffer_len(to_read);
            let n = reader
                .read(&mut chunk[..to_read])
                .map_err(Self::read_error_to_rar)?;
            if n == 0 {
                break;
            }
            chunk.truncate(n);
            writer.write_all(&chunk).map_err(RarError::Io)?;
            written += n as u64;
            stream_hash.submit(chunk).map_err(RarError::Io)?;
        }

        writer.flush().map_err(RarError::Io)?;

        // Use final volume's CRC and HMAC flag if continuations were discovered.
        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        let stream_outputs = stream_hash.finalize().map_err(RarError::Io)?;
        let actual_crc = stream_outputs.crc32;
        let actual_blake = stream_outputs.blake2sp;
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
            .with_member_name(&fh.name)
            .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(
                split_after,
                self.format,
                password
                    .map(str::to_owned)
                    .or_else(|| self.password.clone()),
            )
            .with_kdf_cache(Arc::clone(&self.kdf_cache))
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
        let compute_crc = expected_crc.is_some() || (options.verify && split_after);
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        // Hashing runs off-thread for large members (see hash_pipeline).
        let stream_hash = (compute_crc || compute_blake2)
            .then(|| SharedHashStream::new(compute_crc, false, compute_blake2, unpacked_size));
        let mut hash_writer = HashTrackingWriter {
            inner: writer,
            hash: stream_hash.clone(),
        };

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

        let (actual_crc, actual_blake) = finalize_shared_hash(stream_hash)?;
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
        self.reject_link_member_without_file_target(entry, &fh)?;
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
            .with_member_name(&fh.name)
            .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(
                split_after,
                self.format,
                password
                    .map(str::to_owned)
                    .or_else(|| self.password.clone()),
            )
            .with_kdf_cache(Arc::clone(&self.kdf_cache))
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
        let compute_crc = expected_crc.is_some() || (options.verify && split_after);
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        // Hashing runs off-thread for large members (see hash_pipeline).
        let stream_hash = SharedHashStream::new(compute_crc, false, compute_blake2, _unpacked_size);

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

        loop {
            let to_read =
                store_limit.next_read_len(total_written, STREAMING_STORE_CHUNK_BUFFER_BYTES);
            if to_read == 0 {
                Self::enforce_store_ceiling_not_exceeded(&mut reader, store_limit, &fh.name)?;
                break;
            }
            let mut chunk = stream_hash.take_buffer_len(to_read);
            let n = reader
                .read(&mut chunk[..to_read])
                .map_err(Self::read_error_to_rar)?;
            if n == 0 {
                break;
            }
            chunk.truncate(n);

            // Check for volume transition.
            let new_vol = volume_tracker.load(Ordering::Acquire);
            if new_vol != current_vol {
                current_writer.flush().map_err(RarError::Io)?;
                chunks.push((current_vol, chunk_bytes));
                current_vol = new_vol;
                current_writer = writer_factory(current_vol)?;
                chunk_bytes = 0;
            }

            current_writer.write_all(&chunk).map_err(RarError::Io)?;
            chunk_bytes += n as u64;
            total_written += n as u64;
            stream_hash.submit(chunk).map_err(RarError::Io)?;
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

        let stream_outputs = stream_hash.finalize().map_err(RarError::Io)?;
        let actual_crc = stream_outputs.crc32;
        let actual_blake = stream_outputs.blake2sp;
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

        self.advance_solid_cursor_to_streaming(index, fh, provider, options.password.as_deref())?;

        let volume_tracker = Arc::new(AtomicUsize::new(first_vol));
        let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cont_meta = Rc::new(RefCell::new(ContinuationMetadata::default()));
        let chained = ChainedSegmentReader::new(&segments, provider)
            .with_member_name(&fh.name)
            .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(
                split_after,
                self.format,
                password
                    .map(str::to_owned)
                    .or_else(|| self.password.clone()),
            )
            .with_kdf_cache(Arc::clone(&self.kdf_cache))
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
        let compute_crc = expected_crc.is_some() || (options.verify && split_after);
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        let shared_hash = (compute_crc || compute_blake2)
            .then(|| SharedHashStream::new(compute_crc, false, compute_blake2, unpacked_size));

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
                    shared_hash.clone(),
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
                    shared_hash.clone(),
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
                shared_hash.clone(),
            )?
        };
        self.enforce_unknown_lz_chunk_limit(fh, &chunks)?;

        let final_meta = cont_meta.borrow();
        let effective_crc = final_meta.data_crc32.or(fh.data_crc32);
        let effective_blake = final_meta.blake2_hash.or(expected_blake);
        let final_use_hash_mac = use_hash_mac || final_meta.use_hash_mac;
        drop(final_meta);

        let (actual_crc, actual_blake) = finalize_shared_hash(shared_hash)?;
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
            .with_member_name(&fh.name)
            .with_packed_hash_key(rar5_crypto.as_ref().map(|crypto| crypto.hash_key))
            .with_max_data_segment(self.limits.max_data_segment)
            .with_continuation(
                split_after,
                self.format,
                password
                    .map(str::to_owned)
                    .or_else(|| self.password.clone()),
            )
            .with_kdf_cache(Arc::clone(&self.kdf_cache))
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
        let compute_crc = expected_crc.is_some() || (options.verify && split_after);
        let compute_blake2 = expected_blake.is_some()
            || (options.verify && split_after && self.format == ArchiveFormat::Rar5);
        let shared_hash = (compute_crc || compute_blake2)
            .then(|| SharedHashStream::new(compute_crc, false, compute_blake2, unpacked_size));

        let chunks = if self.format == ArchiveFormat::Rar5 {
            let shared_transitions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let tracking_reader = VolumeTrackingReader::new(
                BufReader::with_capacity(1024 * 1024, inner),
                volume_tracker,
            )
            .with_shared_transitions(Arc::clone(&shared_transitions));

            let hash_clone = shared_hash.clone();
            crate::decompress::lz::decompress_lz_reader_to_writer_chunked_with_max_dict_size(
                tracking_reader,
                unpacked_size,
                &fh.compression,
                first_vol,
                shared_transitions,
                |vol_idx| {
                    let writer = writer_factory(vol_idx)?;
                    if hash_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            hash: hash_clone.clone(),
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

            let hash_clone = shared_hash.clone();
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
                    if hash_clone.is_some() {
                        Ok(Box::new(HashTrackingWriter {
                            inner: writer,
                            hash: hash_clone.clone(),
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

        let (actual_crc, actual_blake) = finalize_shared_hash(shared_hash)?;
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
struct HashingWriter<'a, W: Write + ?Sized> {
    inner: &'a mut W,
    crc: Option<crc32fast::Hasher>,
    blake2: Option<Blake2spHasher>,
}

impl<'a, W: Write + ?Sized> HashingWriter<'a, W> {
    fn new(inner: &'a mut W, compute_crc: bool, compute_blake2: bool) -> Self {
        Self {
            inner,
            crc: compute_crc.then(crc32fast::Hasher::new),
            blake2: compute_blake2.then(Blake2spHasher::new),
        }
    }

    fn finalize_crc(&self) -> u32 {
        self.crc
            .as_ref()
            .map(|hasher| hasher.clone().finalize())
            .unwrap_or(0)
    }

    fn finalize_blake2(&self) -> [u8; 32] {
        self.blake2
            .as_ref()
            .map(|hasher| hasher.finalize())
            .unwrap_or([0; 32])
    }
}

impl<W: Write + ?Sized> Write for HashingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        if let Some(ref mut hasher) = self.crc {
            hasher.update(&buf[..written]);
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

/// Writer wrapper that feeds a shared hash stream while forwarding writes.
struct HashTrackingWriter<W: Write> {
    inner: W,
    hash: Option<Arc<SharedHashStream>>,
}

impl<W: Write> Write for HashTrackingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if let Some(ref hash) = self.hash {
            hash.update(&buf[..n])?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Finalize an optional shared hash stream into (crc32, blake2sp) actuals.
fn finalize_shared_hash(
    shared: Option<Arc<SharedHashStream>>,
) -> RarResult<(Option<u32>, Option<[u8; 32]>)> {
    match shared {
        None => Ok((None, None)),
        Some(hash) => {
            let outputs = hash.finalize().map_err(RarError::Io)?;
            Ok((outputs.crc32, outputs.blake2sp))
        }
    }
}

enum SegmentPackedHashState {
    Crc32(crc32fast::Hasher),
    Blake2sp(Box<Blake2spHasher>),
}

struct SegmentPackedHashVerifier {
    member_name: String,
    volume_index: usize,
    expected: PackedDataHash,
    uses_mac: bool,
    hash_key: Option<[u8; 32]>,
    state: SegmentPackedHashState,
}

impl SegmentPackedHashVerifier {
    fn new(member_name: &str, segment: &DataSegment, hash_key: Option<[u8; 32]>) -> Option<Self> {
        let expected = segment.packed_hash?;
        let state = match expected {
            PackedDataHash::Crc32(_) => SegmentPackedHashState::Crc32(crc32fast::Hasher::new()),
            PackedDataHash::Blake2sp(_) => SegmentPackedHashState::Blake2sp(Box::default()),
        };
        Some(Self {
            member_name: member_name.to_string(),
            volume_index: segment.volume_index,
            expected,
            uses_mac: segment.packed_hash_uses_mac,
            hash_key,
            state,
        })
    }

    fn update(&mut self, data: &[u8]) {
        match &mut self.state {
            SegmentPackedHashState::Crc32(hasher) => hasher.update(data),
            SegmentPackedHashState::Blake2sp(hasher) => hasher.update(data),
        }
    }

    fn finish(self) -> std::io::Result<()> {
        match (self.expected, self.state) {
            (PackedDataHash::Crc32(expected), SegmentPackedHashState::Crc32(hasher)) => {
                let mut actual = hasher.finalize();
                if self.uses_mac {
                    actual = crate::crypto::convert_crc32_to_mac(
                        actual,
                        self.hash_key.as_ref().ok_or_else(|| {
                            packed_hash_io_error(RarError::CorruptArchive {
                                detail: format!(
                                    "packed segment for {} in volume {} is missing a RAR5 hash key",
                                    self.member_name, self.volume_index
                                ),
                            })
                        })?,
                    );
                }
                if actual != expected {
                    return Err(packed_hash_io_error(RarError::PackedDataCrcMismatch {
                        member: self.member_name,
                        volume: self.volume_index,
                        expected,
                        actual,
                    }));
                }
            }
            (PackedDataHash::Blake2sp(expected), SegmentPackedHashState::Blake2sp(hasher)) => {
                let mut actual = hasher.finalize();
                if self.uses_mac {
                    actual = crate::crypto::convert_blake2_to_mac(
                        actual,
                        self.hash_key.as_ref().ok_or_else(|| {
                            packed_hash_io_error(RarError::CorruptArchive {
                                detail: format!(
                                    "packed segment for {} in volume {} is missing a RAR5 hash key",
                                    self.member_name, self.volume_index
                                ),
                            })
                        })?,
                    );
                }
                if actual != expected {
                    return Err(packed_hash_io_error(RarError::PackedDataBlake2Mismatch {
                        member: self.member_name,
                        volume: self.volume_index,
                    }));
                }
            }
            _ => {
                return Err(packed_hash_io_error(RarError::CorruptArchive {
                    detail: format!(
                        "packed segment for {} in volume {} used mismatched hash state",
                        self.member_name, self.volume_index
                    ),
                }));
            }
        }
        Ok(())
    }
}

fn packed_hash_io_error(error: RarError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
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
    member_name: String,
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
    /// Shared KDF cache for encrypted continuation headers.
    kdf_cache: Arc<crate::crypto::KdfCache>,
    /// Shared metadata sink updated when continuation headers are discovered.
    /// The caller holds an Rc clone to read final values after streaming.
    metadata_sink: Option<Rc<RefCell<ContinuationMetadata>>>,
    /// Shared volume index tracker. Updated whenever the reader advances to a
    /// new segment, allowing callers (even behind wrapper layers like
    /// DecryptingReader) to observe which volume is currently being read.
    volume_tracker: Option<Arc<AtomicUsize>>,
    packed_hash_key: Option<[u8; 32]>,
    current_packed_hash: Option<SegmentPackedHashVerifier>,
}

struct ArchiveSegmentReader<'a> {
    volumes: &'a mut [Option<VolumeData>],
    limits: &'a Limits,
    segments: Vec<DataSegment>,
    member_name: String,
    current_seg: usize,
    remaining_in_segment: u64,
    volume_tracker: Option<Arc<AtomicUsize>>,
    packed_hash_key: Option<[u8; 32]>,
    current_packed_hash: Option<SegmentPackedHashVerifier>,
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
            packed_hash_key: None,
            current_packed_hash: None,
        }
    }

    fn with_volume_tracker(mut self, tracker: Arc<AtomicUsize>) -> Self {
        self.volume_tracker = Some(tracker);
        self
    }

    fn with_packed_hash_key(mut self, hash_key: Option<[u8; 32]>) -> Self {
        self.packed_hash_key = hash_key;
        self
    }

    fn finish_current_packed_hash(&mut self) -> std::io::Result<()> {
        if let Some(verifier) = self.current_packed_hash.take() {
            verifier.finish()?;
        }
        Ok(())
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
        self.current_packed_hash =
            SegmentPackedHashVerifier::new(&self.member_name, seg, self.packed_hash_key);
        if self.remaining_in_segment == 0 {
            self.finish_current_packed_hash()?;
        }
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
                if let Some(verifier) = self.current_packed_hash.as_mut() {
                    verifier.update(&buf[..n]);
                }
                if self.remaining_in_segment == 0 {
                    self.finish_current_packed_hash()?;
                }
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
            member_name: "RAR member".to_string(),
            max_data_segment: crate::limits::Limits::default().max_data_segment,
            current_seg: 0,
            current_reader: None,
            remaining_in_segment: 0,
            split_after: false,
            next_discover_vol: next_vol,
            format: ArchiveFormat::Rar5,
            password: None,
            kdf_cache: Arc::new(crate::crypto::KdfCache::new()),
            metadata_sink: None,
            volume_tracker: None,
            packed_hash_key: None,
            current_packed_hash: None,
        }
    }

    pub fn with_max_data_segment(mut self, max_data_segment: u64) -> Self {
        self.max_data_segment = max_data_segment;
        self
    }

    fn with_member_name(mut self, member_name: &str) -> Self {
        self.member_name = member_name.to_string();
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

    fn with_kdf_cache(mut self, kdf_cache: Arc<crate::crypto::KdfCache>) -> Self {
        self.kdf_cache = kdf_cache;
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

    fn with_packed_hash_key(mut self, hash_key: Option<[u8; 32]>) -> Self {
        self.packed_hash_key = hash_key;
        self
    }

    fn finish_current_packed_hash(&mut self) -> std::io::Result<()> {
        if let Some(verifier) = self.current_packed_hash.take() {
            verifier.finish()?;
        }
        Ok(())
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
        self.current_packed_hash =
            SegmentPackedHashVerifier::new(&self.member_name, seg, self.packed_hash_key);
        if self.remaining_in_segment == 0 {
            self.finish_current_packed_hash()?;
        }
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
            let parsed = crate::rar4::parse_rar4_headers_with_kdf_cache(
                &mut reader,
                self.password.as_deref(),
                &self.kdf_cache,
            )
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
                    let file_header =
                        RarArchive::rar4_to_file_header(fh, parsed.archive_header.is_solid);
                    self.segments.push(DataSegment::with_packed_hash(
                        vol_idx,
                        fh.data_offset,
                        fh.packed_size,
                        RarArchive::packed_hash_for_split_segment(&file_header, None),
                        false,
                    ));
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
            let parsed = crate::header::parse_all_headers_with_kdf_cache(
                &mut reader,
                self.password.as_deref(),
                &self.kdf_cache,
            )
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
                    let packed_hash =
                        RarArchive::packed_hash_for_split_segment(&pf.header, pf.hash.as_ref());
                    let packed_hash_uses_mac = pf
                        .file_encryption
                        .as_ref()
                        .is_some_and(|fe| fe.use_hash_mac);
                    self.segments.push(DataSegment::with_packed_hash(
                        vol_idx,
                        pf.header.data_offset,
                        pf.header.data_size,
                        packed_hash,
                        packed_hash_uses_mac,
                    ));
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
                if let Some(verifier) = self.current_packed_hash.as_mut() {
                    verifier.update(&buf[..n]);
                }
                if self.remaining_in_segment == 0 {
                    self.finish_current_packed_hash()?;
                }
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

    fn test_rar3_symlink_header(expected_crc: Option<u32>) -> FileHeader {
        FileHeader {
            name: "link".into(),
            name_raw: Some(b"link".to_vec()),
            unpacked_size: Some(0),
            attributes: FileAttributes(0xA000),
            mtime: None,
            ctime: None,
            atime: None,
            data_crc32: expected_crc,
            data_hash: expected_crc.map(crate::types::DataHash::Crc32),
            compression: CompressionInfo {
                format: ArchiveFormat::Rar4,
                version: 29,
                solid: false,
                method: CompressionMethod::Store,
                dict_size: 0,
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
        }
    }

    #[test]
    fn rar5_comment_text_stops_at_nul_like_unrar() {
        assert_eq!(
            RarArchive::decode_rar5_comment_text(b"comment\0ignored"),
            "comment"
        );
    }

    #[test]
    fn rar5_comment_text_keeps_prefix_before_invalid_utf8_like_unrar() {
        assert_eq!(
            RarArchive::decode_rar5_comment_text(b"hello/\xffignored"),
            "hello/"
        );
    }

    #[test]
    fn rar5_comment_text_decodes_overlong_utf8_like_unrar() {
        assert_eq!(
            RarArchive::decode_rar5_comment_text(b"hello\xc0\xafworld"),
            "hello/world"
        );
    }

    #[test]
    fn rar5_windows_symlink_target_strips_absolute_prefix_like_unrar() {
        assert_eq!(
            RarArchive::normalized_rar5_redirection_target(
                header::RedirectionType::WindowsSymlink,
                "\\??\\C:\\target"
            ),
            "C:/target"
        );
    }

    #[test]
    fn rar5_windows_absolute_symlink_is_relative_after_unix_normalization_like_unrar() {
        let target = RarArchive::normalized_rar5_redirection_target(
            header::RedirectionType::WindowsSymlink,
            "\\??\\C:\\target",
        );

        assert!(crate::path::is_safe_symlink_target_for_archive_member(
            ArchiveFormat::Rar5,
            "link",
            "link",
            &target,
        ));
        assert!(
            matches!(
                RarArchive::ensure_safe_windows_reparse_target("\\??\\C:\\target", "link"),
                Err(RarError::UnsafeLinkTarget { .. })
            ),
            "raw Windows reparse creation still rejects absolute targets by default"
        );
    }

    #[test]
    fn rar5_windows_junction_reparse_names_match_unrar_prefix_rules() {
        let substitute = RarArchive::windows_junction_substitute_name("/??/UNC/server/share");
        assert_eq!(substitute, "\\??\\UNC\\server\\share");
        assert_eq!(
            RarArchive::windows_junction_print_name(&substitute),
            "\\server\\share"
        );

        let relative = RarArchive::windows_junction_substitute_name("dir/target");
        assert_eq!(relative, "dir\\target");
        assert_eq!(
            RarArchive::windows_junction_print_name(&relative),
            "dir\\target"
        );
    }

    #[test]
    fn rar5_windows_junction_reparse_buffer_matches_unrar_layout() {
        let buffer = RarArchive::windows_junction_reparse_buffer("dir/target").unwrap();

        assert_eq!(
            u32::from_le_bytes(buffer[0..4].try_into().unwrap()),
            WINDOWS_IO_REPARSE_TAG_MOUNT_POINT
        );
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[4..6].try_into().unwrap())),
            buffer.len() - 8
        );
        assert_eq!(u16::from_le_bytes(buffer[6..8].try_into().unwrap()), 0);
        assert_eq!(u16::from_le_bytes(buffer[8..10].try_into().unwrap()), 0);

        let substitute = "dir\\target";
        let substitute_bytes = substitute.encode_utf16().count() * 2;
        let print_offset = (substitute.encode_utf16().count() + 1) * 2;
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[10..12].try_into().unwrap())),
            substitute_bytes
        );
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[12..14].try_into().unwrap())),
            print_offset
        );
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[14..16].try_into().unwrap())),
            substitute_bytes
        );
    }

    #[test]
    fn rar5_windows_reparse_absolute_targets_are_blocked_like_unrar_default() {
        for target in [
            "\\??\\C:\\target",
            "\\?\\C:\\target",
            "\\.\\C:\\target",
            "\\rooted",
            "/??/UNC/server/share",
            "/?/C:/target",
            "/rooted",
            "C:\\target",
            "C:/target",
            "\\\\server\\share",
            "//server/share",
        ] {
            assert!(RarArchive::windows_reparse_target_is_absolute(target));
            assert!(
                matches!(
                    RarArchive::ensure_safe_windows_reparse_target(target, "link"),
                    Err(RarError::UnsafeLinkTarget { .. })
                ),
                "expected {target:?} to be rejected"
            );
        }

        for target in [
            "C:relative",
            "dir\\target",
            "dir/target",
            ".\\target",
            "./target",
        ] {
            assert!(!RarArchive::windows_reparse_target_is_absolute(target));
            RarArchive::ensure_safe_windows_reparse_target(target, "link").unwrap();
        }
    }

    #[test]
    fn rar5_windows_symlink_reparse_buffer_matches_unrar_layout() {
        let buffer = RarArchive::windows_symlink_reparse_buffer("dir/target").unwrap();

        assert_eq!(
            u32::from_le_bytes(buffer[0..4].try_into().unwrap()),
            WINDOWS_IO_REPARSE_TAG_SYMLINK
        );
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[4..6].try_into().unwrap())),
            buffer.len() - 8
        );
        assert_eq!(u16::from_le_bytes(buffer[6..8].try_into().unwrap()), 0);
        assert_eq!(u16::from_le_bytes(buffer[8..10].try_into().unwrap()), 0);

        let substitute = "dir\\target";
        let substitute_bytes = substitute.encode_utf16().count() * 2;
        let print_offset = (substitute.encode_utf16().count() + 1) * 2;
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[10..12].try_into().unwrap())),
            substitute_bytes
        );
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[12..14].try_into().unwrap())),
            print_offset
        );
        assert_eq!(
            usize::from(u16::from_le_bytes(buffer[14..16].try_into().unwrap())),
            substitute_bytes
        );
        assert_eq!(
            u32::from_le_bytes(buffer[16..20].try_into().unwrap()),
            WINDOWS_SYMLINK_FLAG_RELATIVE
        );
    }

    #[test]
    fn windows_filetime_interval_conversion_matches_unrar_epoch() {
        assert_eq!(
            RarArchive::windows_filetime_intervals(std::time::UNIX_EPOCH).unwrap(),
            116_444_736_000_000_000
        );
        assert_eq!(
            RarArchive::windows_filetime_intervals(
                std::time::UNIX_EPOCH + std::time::Duration::new(1, 987_654_321)
            )
            .unwrap(),
            116_444_736_019_876_543
        );
    }

    #[test]
    fn unix_output_mode_preserves_supported_unix_and_darwin_modes() {
        let mut fh = test_rar3_symlink_header(None);
        fh.is_directory = false;

        fh.host_os = HostOs::Unix;
        fh.attributes = FileAttributes(0o100755);
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o755));

        fh.host_os = HostOs::Darwin;
        fh.compression.format = ArchiveFormat::Rar5;
        fh.attributes = FileAttributes(0o120700);
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o700));

        fh.attributes = FileAttributes(0);
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0));
    }

    #[test]
    fn unix_output_mode_converts_windows_attributes_like_unrar() {
        let mask = current_umask();
        let mut fh = test_rar3_symlink_header(None);
        fh.host_os = HostOs::Windows;

        fh.is_directory = true;
        fh.attributes = FileAttributes(0x10);
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o777 & !mask));

        fh.is_directory = false;
        fh.attributes = FileAttributes(0x01);
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o444 & !mask));

        fh.attributes = FileAttributes(0x20);
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o666 & !mask));
    }

    #[test]
    fn unix_output_mode_uses_unrar_default_for_unsupported_hosts() {
        let mask = current_umask();
        let mut fh = test_rar3_symlink_header(None);
        fh.host_os = HostOs::Unknown(0);

        fh.is_directory = false;
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o666 & !mask));

        fh.is_directory = true;
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o777 & !mask));

        fh.host_os = HostOs::Darwin;
        fh.compression.format = ArchiveFormat::Rar4;
        fh.attributes = FileAttributes(0o100750);
        fh.is_directory = false;
        assert_eq!(RarArchive::unix_output_mode(&fh), Some(0o666 & !mask));
    }

    #[cfg(unix)]
    #[test]
    fn apply_unix_permissions_uses_supported_os_conversion_on_disk() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mode.bin");
        std::fs::write(&path, b"mode").unwrap();

        let mut fh = test_rar3_symlink_header(None);
        fh.host_os = HostOs::Unix;
        fh.attributes = FileAttributes(0o100750);
        fh.is_directory = false;

        RarArchive::apply_unix_permissions(&fh, &path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o750
        );

        fh.host_os = HostOs::Windows;
        fh.attributes = FileAttributes(0x01);
        RarArchive::apply_unix_permissions(&fh, &path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o444 & !current_umask()
        );

        let dir = temp.path().join("dir");
        std::fs::create_dir(&dir).unwrap();
        fh.is_directory = true;
        fh.attributes = FileAttributes(0x10);
        RarArchive::apply_unix_permissions(&fh, &dir).unwrap();
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o777 & !current_umask()
        );
    }

    #[test]
    fn windows_output_attributes_map_only_supported_os_behavior() {
        let mut fh = test_rar3_symlink_header(None);
        fh.attributes = FileAttributes(0o100755);
        fh.host_os = HostOs::Unix;
        fh.is_directory = false;
        assert_eq!(
            RarArchive::windows_output_attributes(&fh),
            WINDOWS_FILE_ATTRIBUTE_ARCHIVE
        );

        fh.host_os = HostOs::Darwin;
        fh.is_directory = true;
        assert_eq!(
            RarArchive::windows_output_attributes(&fh),
            WINDOWS_FILE_ATTRIBUTE_DIRECTORY
        );

        fh.host_os = HostOs::Unknown(0);
        fh.is_directory = false;
        assert_eq!(
            RarArchive::windows_output_attributes(&fh),
            WINDOWS_FILE_ATTRIBUTE_ARCHIVE
        );

        fh.host_os = HostOs::Windows;
        fh.attributes = FileAttributes(u64::from(
            WINDOWS_FILE_ATTRIBUTE_READONLY
                | WINDOWS_FILE_ATTRIBUTE_HIDDEN
                | WINDOWS_FILE_ATTRIBUTE_ARCHIVE,
        ));
        assert_eq!(
            RarArchive::windows_output_attributes(&fh),
            WINDOWS_FILE_ATTRIBUTE_READONLY
                | WINDOWS_FILE_ATTRIBUTE_HIDDEN
                | WINDOWS_FILE_ATTRIBUTE_ARCHIVE
        );
    }

    #[test]
    fn windows_stream_restore_clears_only_readonly_before_ads_write() {
        let attributes = WINDOWS_FILE_ATTRIBUTE_READONLY
            | WINDOWS_FILE_ATTRIBUTE_HIDDEN
            | WINDOWS_FILE_ATTRIBUTE_ARCHIVE;
        assert_eq!(
            RarArchive::windows_attributes_without_readonly(attributes),
            WINDOWS_FILE_ATTRIBUTE_HIDDEN | WINDOWS_FILE_ATTRIBUTE_ARCHIVE
        );
    }

    #[test]
    fn windows_overwrite_prepare_allows_missing_path_like_unrar_replace_flow() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing.bin");
        RarArchive::clear_windows_readonly_before_overwrite(&missing).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn directory_output_replaces_final_symlink_before_metadata_restore() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("marker"), b"keep").unwrap();
        let out_path = temp.path().join("dir");
        std::os::unix::fs::symlink(&outside, &out_path).unwrap();

        RarArchive::ensure_output_directory_for_member("dir", &out_path).unwrap();

        let metadata = std::fs::symlink_metadata(&out_path).unwrap();
        assert!(metadata.is_dir());
        assert!(!metadata.file_type().is_symlink());
        assert_eq!(std::fs::read(outside.join("marker")).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn directory_output_replaces_parent_symlink_before_creating_child() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("marker"), b"keep").unwrap();
        let parent = temp.path().join("parent");
        std::os::unix::fs::symlink(&outside, &parent).unwrap();
        let out_path = parent.join("child");

        RarArchive::ensure_output_directory_for_member("parent/child", &out_path).unwrap();

        let parent_metadata = std::fs::symlink_metadata(&parent).unwrap();
        assert!(parent_metadata.is_dir());
        assert!(!parent_metadata.file_type().is_symlink());
        assert!(out_path.is_dir());
        assert_eq!(std::fs::read(outside.join("marker")).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn directory_output_retries_with_unrar_windows_share_name_after_create_failure() {
        let temp = tempfile::tempdir().unwrap();
        let original = temp.path().join("bad?dir");
        let fallback = temp.path().join("bad_dir");
        std::fs::write(&original, b"existing file").unwrap();

        let resolved =
            RarArchive::ensure_output_directory_for_member("bad?dir", &original).unwrap();

        assert_eq!(resolved, fallback);
        assert_eq!(std::fs::read(&original).unwrap(), b"existing file");
        assert!(fallback.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn regular_output_retries_with_unrar_windows_share_name_after_create_failure() {
        use std::io::Write;

        let temp = tempfile::tempdir().unwrap();
        let original = temp.path().join("bad?file.bin");
        let fallback = temp.path().join("bad_file.bin");
        std::fs::create_dir(&original).unwrap();

        let (mut file, resolved) =
            RarArchive::create_regular_output_file_for_member("bad?file.bin", &original).unwrap();
        file.write_all(b"fallback bytes").unwrap();
        drop(file);

        assert_eq!(resolved, fallback);
        assert!(original.is_dir());
        assert_eq!(std::fs::read(fallback).unwrap(), b"fallback bytes");
    }

    #[test]
    fn unix_owner_chown_link_following_matches_unrar_platform_branch() {
        assert!(RarArchive::unix_owner_chown_follows_links(true));
        assert_eq!(
            RarArchive::unix_owner_chown_follows_links(false),
            cfg!(target_vendor = "apple")
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_owner_restore_failure_is_best_effort_like_unrar() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing.bin");
        let owner = crate::types::UnixOwnerInfo {
            uid: Some(0),
            gid: Some(0),
            ..Default::default()
        };

        RarArchive::apply_unix_owner(Some(&owner), &missing, true).unwrap();
    }

    #[test]
    fn unix_regular_file_times_use_now_for_missing_counterpart_like_unrar() {
        let mtime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000);
        let atime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2_000);
        let now = std::time::UNIX_EPOCH + std::time::Duration::from_secs(3_000);

        let (actual_atime, actual_mtime) =
            RarArchive::unix_regular_file_times(Some(mtime), None, now).unwrap();
        assert_eq!(actual_atime, filetime::FileTime::from_system_time(now));
        assert_eq!(actual_mtime, filetime::FileTime::from_system_time(mtime));

        let (actual_atime, actual_mtime) =
            RarArchive::unix_regular_file_times(None, Some(atime), now).unwrap();
        assert_eq!(actual_atime, filetime::FileTime::from_system_time(atime));
        assert_eq!(actual_mtime, filetime::FileTime::from_system_time(now));

        let (actual_atime, actual_mtime) =
            RarArchive::unix_regular_file_times(Some(mtime), Some(atime), now).unwrap();
        assert_eq!(actual_atime, filetime::FileTime::from_system_time(atime));
        assert_eq!(actual_mtime, filetime::FileTime::from_system_time(mtime));

        assert!(RarArchive::unix_regular_file_times(None, None, now).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn regular_output_timestamp_failures_are_best_effort_like_unrar() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing.bin");
        let mut fh = test_rar3_symlink_header(None);
        fh.mtime = Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000));

        RarArchive::apply_output_times(&fh, &missing).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unix_owner_mode_restore_round_trips_followed_mode_like_unrar() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mode.bin");
        std::fs::write(&path, b"mode").unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o640);
        std::fs::set_permissions(&path, permissions).unwrap();

        let original = RarArchive::unix_followed_mode(&path).unwrap();
        let mut changed = std::fs::metadata(&path).unwrap().permissions();
        changed.set_mode(0o600);
        std::fs::set_permissions(&path, changed).unwrap();

        RarArchive::restore_unix_followed_mode(&path, original).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[test]
    fn rar5_hardlink_target_keeps_prefix_after_slash_to_native_like_unrar() {
        assert_eq!(
            RarArchive::normalized_rar5_redirection_target(
                header::RedirectionType::Hardlink,
                "\\??\\C:\\target"
            ),
            "??/C:/target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn hardlink_output_restores_shared_inode_attributes_like_unrar() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        let out_path = temp.path().join("copy.bin");
        std::fs::write(&source, b"linked bytes").unwrap();
        let mut permissions = std::fs::metadata(&source).unwrap().permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(&source, permissions).unwrap();

        let mut fh = test_rar3_symlink_header(None);
        fh.attributes = FileAttributes(0o777);
        fh.host_os = HostOs::Unix;

        RarArchive::create_hardlink_output(&fh, "copy.bin", "copy.bin", "source.bin", &out_path)
            .unwrap();

        assert_eq!(std::fs::read(&out_path).unwrap(), b"linked bytes");
        assert_eq!(
            std::fs::metadata(&source).unwrap().permissions().mode() & 0o777,
            0o777
        );
        assert_eq!(
            std::fs::metadata(&out_path).unwrap().permissions().mode() & 0o777,
            0o777
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn rar5_file_reference_drive_prefix_is_relative_like_unrar_on_unix() {
        assert_eq!(
            RarArchive::normalized_rar5_redirection_target(
                header::RedirectionType::FileCopy,
                "C:\\dir\\source.txt"
            ),
            "C:/dir/source.txt"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn rar5_file_reference_target_path_preserves_drive_relative_prefix_on_unix() {
        assert_eq!(
            RarArchive::redirection_target_path(
                "copy.bin",
                "C:/dir/source.txt",
                std::path::Path::new("/tmp/out/copy.bin"),
            ),
            std::path::Path::new("/tmp/out/C:/dir/source.txt")
        );
    }

    #[test]
    fn rar5_filecopy_target_uses_slash_to_native_and_convert_path_like_unrar() {
        assert_eq!(
            RarArchive::normalized_rar5_redirection_target(
                header::RedirectionType::FileCopy,
                "old\\..\\dir\\source.txt"
            ),
            "dir/source.txt"
        );
    }

    #[test]
    fn rar5_filecopy_ignores_dir_target_flag_like_unrar() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        let out_path = temp.path().join("copy.bin");
        std::fs::write(&source, b"copied bytes").unwrap();

        let mut archive = empty_rar5_archive();
        let mut fh = test_rar3_symlink_header(None);
        fh.name = "copy.bin".to_string();
        fh.attributes = FileAttributes(0o100644);
        fh.host_os = HostOs::Unix;
        fh.compression.format = ArchiveFormat::Rar5;

        let written = archive
            .extract_link_member_to_file(
                0,
                &ExtractOptions::default(),
                &fh,
                None,
                Some(header::Redirection {
                    redir_type: header::RedirectionType::FileCopy,
                    target: "source.bin".to_string(),
                    target_raw: Some(b"source.bin".to_vec()),
                    target_is_directory: true,
                }),
                &out_path,
            )
            .unwrap();

        assert_eq!(written, Some(b"copied bytes".len() as u64));
        assert_eq!(std::fs::read(out_path).unwrap(), b"copied bytes");
    }

    #[cfg(not(windows))]
    #[test]
    fn rar5_unix_symlink_uses_unrar_target_bytes() {
        use std::os::unix::ffi::OsStrExt;

        let temp = tempfile::tempdir().unwrap();
        let out_path = temp.path().join("link.bin");

        let mut archive = empty_rar5_archive();
        let mut fh = test_rar3_symlink_header(None);
        fh.name = "link.bin".to_string();
        fh.attributes = FileAttributes(0);
        fh.host_os = HostOs::Unix;
        fh.compression.format = ArchiveFormat::Rar5;

        let written = archive
            .extract_link_member_to_file(
                0,
                &ExtractOptions::default(),
                &fh,
                None,
                Some(header::Redirection {
                    redir_type: header::RedirectionType::UnixSymlink,
                    target: "safe\u{fffd}target".to_string(),
                    target_raw: Some(b"safe\xed\xa0\x80target".to_vec()),
                    target_is_directory: false,
                }),
                &out_path,
            )
            .unwrap();

        assert_eq!(written, Some(0));
        let link = std::fs::read_link(out_path).unwrap();
        assert_eq!(link.as_os_str().as_bytes(), b"safe\xed\xa0\x80target");
    }

    #[cfg(unix)]
    #[test]
    fn rar5_windows_junction_extracts_as_symlink_on_unix_like_unrar() {
        use std::os::unix::ffi::OsStrExt;

        let temp = tempfile::tempdir().unwrap();
        let out_path = temp.path().join("junction");

        let mut archive = empty_rar5_archive();
        let mut fh = test_rar3_symlink_header(None);
        fh.name = "junction".to_string();
        fh.attributes = FileAttributes(0);
        fh.host_os = HostOs::Windows;
        fh.compression.format = ArchiveFormat::Rar5;
        fh.is_directory = true;

        let written = archive
            .extract_link_member_to_file(
                0,
                &ExtractOptions::default(),
                &fh,
                None,
                Some(header::Redirection {
                    redir_type: header::RedirectionType::WindowsJunction,
                    target: "safe\\target".to_string(),
                    target_raw: Some(b"safe\\target".to_vec()),
                    target_is_directory: true,
                }),
                &out_path,
            )
            .unwrap();

        assert_eq!(written, Some(0));
        let link = std::fs::read_link(out_path).unwrap();
        assert_eq!(link.as_os_str().as_bytes(), b"safe/target");
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
            original_name_raw: None,
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

    fn test_rar5_service(name: &str, unpacked_size: u64, data_size: u64) -> ServiceEntry {
        ServiceEntry {
            header_offset: 0,
            is_child: false,
            is_inherited: false,
            file_header: FileHeader {
                name: name.to_string(),
                name_raw: Some(name.as_bytes().to_vec()),
                unpacked_size: Some(unpacked_size),
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
                    method: CompressionMethod::Store,
                    dict_size: 0,
                },
                host_os: HostOs::Unix,
                is_directory: false,
                file_flags: 0,
                data_size,
                split_before: false,
                split_after: false,
                data_offset: 0,
                is_encrypted: false,
                version: None,
                service_subdata: None,
            },
            is_encrypted: false,
            file_encryption: None,
            rar4_salt: None,
            hash: None,
            comment_crc16: None,
            ntfs_stream_name: None,
            segments: vec![DataSegment::new(0, 0, data_size)],
        }
    }

    fn archive_with_single_volume(data: &[u8]) -> RarArchive {
        let mut archive = empty_rar5_archive();
        archive.volumes = vec![Some(VolumeData {
            reader: Box::new(Cursor::new(data.to_vec())),
        })];
        archive
    }

    #[test]
    fn ntfs_stream_name_rules_match_unrar() {
        assert!(RarArchive::is_allowed_ntfs_stream_name(":Zone.Identifier"));
        assert!(RarArchive::is_allowed_ntfs_stream_name(":stream"));

        assert!(!RarArchive::is_allowed_ntfs_stream_name("stream"));
        assert!(RarArchive::is_broken_ntfs_stream_name("stream"));
        assert!(!RarArchive::is_allowed_ntfs_stream_name(":dir/stream"));
        assert!(RarArchive::is_broken_ntfs_stream_name(":dir/stream"));
        assert!(!RarArchive::is_allowed_ntfs_stream_name(":dir\\stream"));
        assert!(RarArchive::is_broken_ntfs_stream_name(":dir\\stream"));

        assert!(!RarArchive::is_allowed_ntfs_stream_name(
            ":Zone.Identifier:$DATA"
        ));
        assert!(RarArchive::is_prohibited_ntfs_stream_name(
            ":Zone.Identifier:$DATA"
        ));
        assert!(RarArchive::is_prohibited_ntfs_stream_name("::$DATA"));

        let mut service = test_rar5_service("STM", 0, 0);
        service.file_header.service_subdata = Some(b":safe\0ignored".to_vec());
        assert_eq!(
            RarArchive::ntfs_stream_name_from_service(&service).as_deref(),
            Some(":safe")
        );

        service.file_header.service_subdata = None;
        service.ntfs_stream_name = Some(":old-stream".to_string());
        assert_eq!(
            RarArchive::ntfs_stream_name_from_service(&service).as_deref(),
            Some(":old-stream")
        );
    }

    #[test]
    fn broken_ntfs_stream_name_policy_matches_unrar_windows_scope() {
        assert_eq!(
            RarArchive::broken_ntfs_stream_name_is_fatal(),
            cfg!(windows)
        );

        let err = RarArchive::broken_ntfs_stream_name_error("file.txt", ":dir/stream");
        assert!(matches!(err, RarError::CorruptArchive { ref detail }
            if detail.contains("broken NTFS alternate stream name")
                && detail.contains("file.txt")
                && detail.contains(":dir/stream")));
    }

    #[test]
    fn ntfs_stream_service_payload_streams_and_verifies_crc() {
        let payload = b"alternate stream bytes";
        let mut archive = archive_with_single_volume(payload);
        let mut service = test_rar5_service("STM", payload.len() as u64, payload.len() as u64);
        service.file_header.service_subdata = Some(b":stream".to_vec());
        service.file_header.data_crc32 = Some(crc32fast::hash(payload));

        let mut out = Vec::new();
        let written = archive
            .write_service_subdata_to_writer(&service, &ExtractOptions::default(), &mut out)
            .unwrap();

        assert_eq!(written, payload.len() as u64);
        assert_eq!(out, payload);
    }

    #[test]
    fn rar4_encrypted_service_payload_uses_legacy_decryptor() {
        let password = "service-password";
        let payload = b"rar4 service dat";
        let salt = *b"svc-salt";
        let (key, iv) = crate::crypto::KdfCache::default().derive_key_rar4(password, Some(&salt));
        let encrypted = crate::crypto::encrypt_aes128_cbc_for_test(&key, &iv, payload);

        let mut service = test_rar5_service("CMT", payload.len() as u64, encrypted.len() as u64);
        service.file_header.compression.format = ArchiveFormat::Rar4;
        service.file_header.compression.version = 29;
        service.file_header.data_crc32 = Some(crc32fast::hash(payload));
        service.file_header.is_encrypted = true;
        service.is_encrypted = true;
        service.rar4_salt = Some(salt);

        let mut archive = archive_with_single_volume(&encrypted);
        archive.set_password(password);
        assert_eq!(
            archive.read_service_subdata_to_memory(&service).unwrap(),
            payload
        );

        let mut archive = archive_with_single_volume(&encrypted);
        let mut out = Vec::new();
        let written = archive
            .write_service_subdata_to_writer(
                &service,
                &ExtractOptions {
                    password: Some(password.to_string()),
                    ..ExtractOptions::default()
                },
                &mut out,
            )
            .unwrap();

        assert_eq!(written, payload.len() as u64);
        assert_eq!(out, payload);
    }

    #[test]
    fn ntfs_stream_service_payload_crc_mismatch_fails() {
        let payload = b"alternate stream bytes";
        let mut archive = archive_with_single_volume(payload);
        let mut service = test_rar5_service("STM", payload.len() as u64, payload.len() as u64);
        service.file_header.service_subdata = Some(b":stream".to_vec());
        service.file_header.data_crc32 = Some(0xDEAD_BEEF);

        let mut out = Vec::new();
        let err = archive
            .write_service_subdata_to_writer(&service, &ExtractOptions::default(), &mut out)
            .unwrap_err();

        assert!(matches!(err, RarError::DataCrcMismatch { .. }));
    }

    #[test]
    fn ntfs_acl_service_payload_streams_and_verifies_crc() {
        let payload = b"self-relative security descriptor bytes";
        let mut archive = archive_with_single_volume(payload);
        let mut service = test_rar5_service("ACL", payload.len() as u64, payload.len() as u64);
        service.file_header.data_crc32 = Some(crc32fast::hash(payload));

        let mut out = Vec::new();
        let written = archive
            .write_service_subdata_to_writer(&service, &ExtractOptions::default(), &mut out)
            .unwrap();

        assert_eq!(written, payload.len() as u64);
        assert_eq!(out, payload);
    }

    #[test]
    fn ntfs_acl_security_information_matches_unrar_privilege_gate() {
        let base = WINDOWS_OWNER_SECURITY_INFORMATION
            | WINDOWS_GROUP_SECURITY_INFORMATION
            | WINDOWS_DACL_SECURITY_INFORMATION;
        assert_eq!(RarArchive::ntfs_acl_security_information(false), base);
        assert_eq!(
            RarArchive::ntfs_acl_security_information(true),
            base | WINDOWS_SACL_SECURITY_INFORMATION
        );
    }

    #[test]
    fn ntfs_acl_long_path_fallback_matches_unrar_get_win_long_path() {
        assert_eq!(
            RarArchive::windows_long_path_string("C:/deep/path/file.txt", "D:\\extract"),
            Some("\\\\?\\C:\\deep\\path\\file.txt".to_string())
        );
        assert_eq!(
            RarArchive::windows_long_path_string("\\\\server/share/file.txt", "D:\\extract"),
            Some("\\\\?\\UNC\\server\\share\\file.txt".to_string())
        );
        assert_eq!(
            RarArchive::windows_long_path_string("\\rooted\\file.txt", "D:\\extract"),
            Some("\\\\?\\D:\\rooted\\file.txt".to_string())
        );
        assert_eq!(
            RarArchive::windows_long_path_string(".\\nested\\file.txt", "D:\\extract"),
            Some("\\\\?\\D:\\extract\\nested\\file.txt".to_string())
        );
        assert_eq!(
            RarArchive::windows_long_path_string("nested\\file.txt", "D:\\extract\\"),
            Some("\\\\?\\D:\\extract\\nested\\file.txt".to_string())
        );
        assert_eq!(
            RarArchive::windows_long_path_string("\\\\?\\C:\\already\\long.txt", "D:\\extract"),
            Some("\\\\?\\C:\\already\\long.txt".to_string())
        );
        assert_eq!(
            RarArchive::windows_long_path_string("", "D:\\extract"),
            None
        );
    }

    #[test]
    fn windows_create_dir_special_names_match_unrar_detection() {
        assert!(RarArchive::windows_path_ends_with_dot_or_space(
            std::path::Path::new("dir.")
        ));
        assert!(RarArchive::windows_path_ends_with_dot_or_space(
            std::path::Path::new("dir ")
        ));
        assert!(!RarArchive::windows_path_ends_with_dot_or_space(
            std::path::Path::new("dir")
        ));
        assert!(!RarArchive::windows_path_ends_with_dot_or_space(
            std::path::Path::new("")
        ));
    }

    #[test]
    fn ntfs_acl_service_payload_crc_mismatch_fails() {
        let payload = b"self-relative security descriptor bytes";
        let mut archive = archive_with_single_volume(payload);
        let mut service = test_rar5_service("ACL", payload.len() as u64, payload.len() as u64);
        service.file_header.data_crc32 = Some(0xDEAD_BEEF);

        let mut out = Vec::new();
        let err = archive
            .write_service_subdata_to_writer(&service, &ExtractOptions::default(), &mut out)
            .unwrap_err();

        assert!(matches!(err, RarError::DataCrcMismatch { .. }));
    }

    #[test]
    fn empty_comment_service_returns_none_like_unrar() {
        let mut archive = empty_rar5_archive();
        archive.services.push(test_rar5_service("CMT", 42, 0));

        assert_eq!(archive.comment().unwrap(), None);
    }

    #[test]
    fn zero_packed_service_payload_returns_empty_like_unrar() {
        let mut archive = empty_rar5_archive();
        let service = test_rar5_service("UOW", 42, 0);

        assert_eq!(
            archive.read_service_subdata_to_memory(&service).unwrap(),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn chained_segment_reader_rejects_oversized_segment_before_reading() {
        let provider = TestVolumeProvider { data: vec![0; 8] };
        let segments = [DataSegment::new(0, 0, 9)];
        let mut reader = ChainedSegmentReader::new(&segments, &provider).with_max_data_segment(8);

        let mut buf = [0u8; 1];
        let err = reader.read(&mut buf).unwrap_err();
        assert!(err.to_string().contains("exceeds limit 8"));
    }

    #[test]
    fn non_file_extraction_rejects_rar3_unix_symlink_member() {
        let mut archive = empty_rar5_archive();
        archive.format = ArchiveFormat::Rar4;
        let fh = test_rar3_symlink_header(None);
        let entry = MemberEntry {
            file_header: fh.clone(),
            is_encrypted: false,
            file_encryption: None,
            rar4_salt: None,
            hash: None,
            redirection: None,
            owner: None,
            segments: Vec::new(),
        };

        let err = archive
            .reject_link_member_without_file_target(&entry, &fh)
            .unwrap_err();

        assert!(
            matches!(
                err,
                RarError::UnsupportedLinkType {
                    ref member,
                    ref link_type
                } if member == "link" && link_type.contains("requires extract_member_to_file")
            ),
            "expected UnsupportedLinkType, got {err}"
        );
    }

    #[test]
    fn rar3_symlink_payload_reader_bypasses_non_file_extraction_guard() {
        let target = b"safe/target";
        let mut archive = empty_rar5_archive();
        archive.format = ArchiveFormat::Rar4;
        archive.volumes.push(Some(VolumeData {
            reader: Box::new(Cursor::new(target.to_vec())),
        }));

        let mut fh = test_rar3_symlink_header(Some(crc32fast::hash(target)));
        fh.unpacked_size = Some(target.len() as u64);
        fh.data_size = target.len() as u64;
        archive.members.push(MemberEntry {
            file_header: fh.clone(),
            is_encrypted: false,
            file_encryption: None,
            rar4_salt: None,
            hash: None,
            redirection: None,
            owner: None,
            segments: vec![DataSegment::new(0, 0, target.len() as u64)],
        });

        let decoded = archive
            .link_target_from_rar3_payload(0, &ExtractOptions::default(), &fh)
            .unwrap();

        assert_eq!(decoded.raw, b"safe/target");
        assert_eq!(decoded.safety_target, "safe/target");
    }

    #[test]
    fn rar3_symlink_target_rejects_oversized_packed_target_before_reading_like_unrar() {
        let mut archive = empty_rar5_archive();
        archive.format = ArchiveFormat::Rar4;
        let mut fh = test_rar3_symlink_header(None);
        fh.data_size = MAX_LINK_TARGET_BYTES as u64 + 1;
        fh.unpacked_size = Some(fh.data_size);

        let err = archive
            .link_target_from_rar3_payload(0, &ExtractOptions::default(), &fh)
            .unwrap_err();

        assert!(
            matches!(err, RarError::ResourceLimit { ref detail } if detail.contains("MAXPATHSIZE")),
            "expected ResourceLimit, got {err}"
        );
    }

    #[test]
    fn rar3_symlink_target_truncates_at_nul_and_hashes_visible_target_like_unrar() {
        let target = b"safe/target";
        let fh = test_rar3_symlink_header(Some(crc32fast::hash(target)));

        let decoded =
            RarArchive::decode_rar3_unix_link_target_payload(&fh, b"safe/target\0ignored", true)
                .unwrap()
                .safety_target;

        assert_eq!(decoded, "safe/target");
    }

    #[test]
    fn rar3_symlink_target_preserves_raw_bytes_after_unrar_safety_decode() {
        let fh = test_rar3_symlink_header(None);

        let decoded =
            RarArchive::decode_rar3_unix_link_target_payload(&fh, b"\xc1\x81-target", false)
                .unwrap();

        assert_eq!(decoded.raw, b"\xc1\x81-target");
        assert_eq!(decoded.safety_target, "A-target");
    }

    #[test]
    fn rar3_symlink_target_accepts_utf8_surrogate_values_like_unrar_safety_decode() {
        let fh = test_rar3_symlink_header(None);

        let decoded =
            RarArchive::decode_rar3_unix_link_target_payload(&fh, b"safe-\xed\xa0\x80", false)
                .unwrap();

        assert_eq!(decoded.raw, b"safe-\xed\xa0\x80");
        assert_eq!(decoded.safety_target, "safe-\u{fffd}");
    }

    #[test]
    fn rar3_symlink_target_rejects_decoded_path_metacharacter_mismatch_like_unrar() {
        let fh = test_rar3_symlink_header(None);

        let err =
            RarArchive::decode_rar3_unix_link_target_payload(&fh, b"safe\xc0\xaftarget", false)
                .unwrap_err();

        assert!(
            matches!(err, RarError::CorruptArchive { ref detail } if detail.contains("path metacharacters")),
            "expected path metacharacter CorruptArchive, got {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rar3_symlink_output_creates_raw_byte_target_like_unrar() {
        use std::os::unix::ffi::OsStrExt;

        let temp = tempfile::tempdir().unwrap();
        let out_path = temp.path().join("link");
        let fh = test_rar3_symlink_header(None);
        let target = Rar3UnixLinkTarget {
            raw: b"\xc1\x81-target".to_vec(),
            safety_target: "A-target".to_string(),
        };

        RarArchive::create_rar3_unix_symlink_output(
            &fh,
            None,
            &ExtractOptions::default(),
            "link",
            "link",
            &target,
            &out_path,
        )
        .unwrap();

        let actual = std::fs::read_link(out_path).unwrap();
        assert_eq!(actual.as_os_str().as_bytes(), b"\xc1\x81-target");
    }

    #[cfg(unix)]
    #[test]
    fn rar3_symlink_output_restores_symlink_mtime_like_unrar() {
        let temp = tempfile::tempdir().unwrap();
        let out_path = temp.path().join("link");
        let expected_mtime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(123_456);
        let mut fh = test_rar3_symlink_header(None);
        fh.mtime = Some(expected_mtime);
        let target = Rar3UnixLinkTarget {
            raw: b"target".to_vec(),
            safety_target: "target".to_string(),
        };

        RarArchive::create_rar3_unix_symlink_output(
            &fh,
            None,
            &ExtractOptions::default(),
            "link",
            "link",
            &target,
            &out_path,
        )
        .unwrap();

        let actual_mtime = std::fs::symlink_metadata(&out_path)
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(actual_mtime, expected_mtime);
    }

    #[test]
    fn rar3_symlink_target_rejects_empty_target_like_unrar() {
        let fh = test_rar3_symlink_header(None);

        let err =
            RarArchive::decode_rar3_unix_link_target_payload(&fh, b"\0ignored", false).unwrap_err();

        assert!(
            matches!(err, RarError::CorruptArchive { ref detail } if detail.contains("empty")),
            "expected empty target CorruptArchive, got {err}"
        );
    }

    #[test]
    fn rar3_symlink_target_rejects_invalid_utf8_like_unrar_safe_char_to_wide() {
        let fh = test_rar3_symlink_header(None);

        let err = RarArchive::decode_rar3_unix_link_target_payload(&fh, b"safe/\xfftarget", false)
            .unwrap_err();

        assert!(
            matches!(err, RarError::CorruptArchive { ref detail } if detail.contains("valid UTF-8")),
            "expected invalid UTF-8 CorruptArchive, got {err}"
        );
    }

    #[test]
    fn archive_member_limits_use_effective_rar4_dictionary_size() {
        let fh = FileHeader {
            name: "rar4-small-dict.bin".into(),
            name_raw: Some(b"rar4-small-dict.bin".to_vec()),
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
            name_raw: Some(b"rar5-small-dict.bin".to_vec()),
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
