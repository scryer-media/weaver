//! Serializable archive header cache for journal persistence.
//!
//! Enables reconstructing a `RarArchive` without reading volume 0 from disk.
//! Headers are serialized as named MessagePack maps so added metadata fields
//! remain backward-compatible.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{
    DataSegment, FileEncryptionInfo, MemberEntry, PackedDataHash, RarArchive, ServiceEntry,
};
use crate::header::file::FileHeader;
use crate::header::{Redirection, RedirectionType};
use crate::limits::Limits;
use crate::types::{
    ArchiveFormat, CompressionInfo, CompressionMethod, FileAttributes, FileHash, HostOs,
    RecoveryRecordInfo, UnixOwnerInfo,
};
use crate::volume::VolumeSet;

/// Serializable snapshot of parsed archive headers.
#[derive(Serialize, Deserialize)]
pub struct CachedArchiveHeaders {
    pub format: u8, // 4=Rar4, 5=Rar5
    pub is_solid: bool,
    pub is_encrypted: bool,
    #[serde(default)]
    pub has_recovery_record: bool,
    #[serde(default)]
    pub recovery_records: Vec<RecoveryRecordInfo>,
    #[serde(default)]
    pub is_locked: bool,
    #[serde(default)]
    pub has_authenticity_verification: bool,
    #[serde(default)]
    pub has_locator: bool,
    #[serde(default)]
    pub quick_open_offset: Option<u64>,
    #[serde(default)]
    pub recovery_record_offset: Option<u64>,
    #[serde(default)]
    pub original_name: Option<String>,
    #[serde(default)]
    pub original_name_raw: Option<Vec<u8>>,
    #[serde(default)]
    pub original_creation_time_ns: Option<u64>,
    pub more_volumes: bool,
    #[serde(default)]
    pub volume_presence: Vec<bool>,
    #[serde(default)]
    pub last_volume_seen: bool,
    pub members: Vec<CachedMember>,
    #[serde(default)]
    pub services: Vec<CachedService>,
}

#[derive(Serialize, Deserialize)]
pub struct CachedMember {
    pub name: String,
    #[serde(default)]
    pub name_raw: Option<Vec<u8>>,
    pub unpacked_size: Option<u64>,
    #[serde(default)]
    pub mtime_ns: Option<u64>,
    #[serde(default)]
    pub ctime_ns: Option<u64>,
    #[serde(default)]
    pub atime_ns: Option<u64>,
    pub data_crc32: Option<u32>,
    pub compression_method: u8,
    #[serde(default)]
    pub compression_version: u8,
    pub compression_solid: bool,
    pub dict_size: u64,
    pub split_before: bool,
    pub split_after: bool,
    pub is_directory: bool,
    pub is_encrypted: bool,
    pub encryption: Option<CachedEncryption>,
    pub rar4_salt: Option<[u8; 8]>,
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub blake2_hash: Option<[u8; 32]>,
    #[serde(default)]
    pub redirection_type: Option<u64>,
    #[serde(default)]
    pub redirection_target: Option<String>,
    #[serde(default)]
    pub redirection_target_raw: Option<Vec<u8>>,
    #[serde(default)]
    pub redirection_target_is_directory: bool,
    #[serde(default)]
    pub attributes: u64,
    #[serde(default = "default_host_os_code")]
    pub host_os: u64,
    #[serde(default)]
    pub owner_user_name: Option<String>,
    #[serde(default)]
    pub owner_group_name: Option<String>,
    #[serde(default)]
    pub owner_user_name_raw: Option<Vec<u8>>,
    #[serde(default)]
    pub owner_group_name_raw: Option<Vec<u8>>,
    #[serde(default)]
    pub owner_uid: Option<u64>,
    #[serde(default)]
    pub owner_gid: Option<u64>,
    pub segments: Vec<CachedSegment>,
}

#[derive(Serialize, Deserialize)]
pub struct CachedService {
    #[serde(default)]
    pub header_offset: u64,
    pub name: String,
    #[serde(default)]
    pub name_raw: Option<Vec<u8>>,
    pub unpacked_size: Option<u64>,
    #[serde(default)]
    pub mtime_ns: Option<u64>,
    #[serde(default)]
    pub ctime_ns: Option<u64>,
    #[serde(default)]
    pub atime_ns: Option<u64>,
    pub data_crc32: Option<u32>,
    pub compression_method: u8,
    #[serde(default)]
    pub compression_version: u8,
    pub compression_solid: bool,
    pub dict_size: u64,
    pub split_before: bool,
    pub split_after: bool,
    pub is_encrypted: bool,
    pub encryption: Option<CachedEncryption>,
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub blake2_hash: Option<[u8; 32]>,
    #[serde(default)]
    pub comment_crc16: Option<u16>,
    #[serde(default)]
    pub attributes: u64,
    #[serde(default = "default_host_os_code")]
    pub host_os: u64,
    #[serde(default)]
    pub service_subdata: Option<Vec<u8>>,
    pub segments: Vec<CachedSegment>,
}

#[derive(Serialize, Deserialize)]
pub struct CachedEncryption {
    #[serde(default)]
    pub version: u64,
    pub kdf_count: u8,
    pub salt: [u8; 16],
    pub iv: [u8; 16],
    pub check_data: Option<[u8; 12]>,
    pub use_hash_mac: bool,
}

#[derive(Serialize, Deserialize)]
pub struct CachedSegment {
    pub volume_index: usize,
    pub data_offset: u64,
    pub data_size: u64,
    #[serde(default)]
    pub packed_crc32: Option<u32>,
    #[serde(default)]
    pub packed_blake2_hash: Option<[u8; 32]>,
    #[serde(default)]
    pub packed_hash_uses_mac: bool,
}

#[derive(Deserialize)]
struct LegacyCachedArchiveHeaders {
    format: u8,
    is_solid: bool,
    is_encrypted: bool,
    more_volumes: bool,
    #[serde(default)]
    volume_presence: Vec<bool>,
    #[serde(default)]
    last_volume_seen: bool,
    members: Vec<LegacyCachedMember>,
}

#[derive(Deserialize)]
struct LegacyCachedMember {
    name: String,
    unpacked_size: Option<u64>,
    data_crc32: Option<u32>,
    compression_method: u8,
    compression_version: u8,
    compression_solid: bool,
    dict_size: u64,
    split_before: bool,
    split_after: bool,
    is_directory: bool,
    is_encrypted: bool,
    encryption: Option<CachedEncryption>,
    rar4_salt: Option<[u8; 8]>,
    blake2_hash: Option<[u8; 32]>,
    segments: Vec<CachedSegment>,
}

impl From<LegacyCachedArchiveHeaders> for CachedArchiveHeaders {
    fn from(value: LegacyCachedArchiveHeaders) -> Self {
        Self {
            format: value.format,
            is_solid: value.is_solid,
            is_encrypted: value.is_encrypted,
            has_recovery_record: false,
            is_locked: false,
            has_authenticity_verification: false,
            has_locator: false,
            quick_open_offset: None,
            recovery_record_offset: None,
            original_name: None,
            original_name_raw: None,
            original_creation_time_ns: None,
            more_volumes: value.more_volumes,
            volume_presence: value.volume_presence,
            last_volume_seen: value.last_volume_seen,
            recovery_records: Vec::new(),
            members: value.members.into_iter().map(CachedMember::from).collect(),
            services: Vec::new(),
        }
    }
}

impl From<LegacyCachedMember> for CachedMember {
    fn from(value: LegacyCachedMember) -> Self {
        Self {
            name: value.name,
            name_raw: None,
            unpacked_size: value.unpacked_size,
            mtime_ns: None,
            ctime_ns: None,
            atime_ns: None,
            data_crc32: value.data_crc32,
            compression_method: value.compression_method,
            compression_version: value.compression_version,
            compression_solid: value.compression_solid,
            dict_size: value.dict_size,
            split_before: value.split_before,
            split_after: value.split_after,
            is_directory: value.is_directory,
            is_encrypted: value.is_encrypted,
            encryption: value.encryption,
            rar4_salt: value.rar4_salt,
            version: None,
            blake2_hash: value.blake2_hash,
            redirection_type: None,
            redirection_target: None,
            redirection_target_raw: None,
            redirection_target_is_directory: false,
            attributes: 0,
            host_os: default_host_os_code(),
            owner_user_name: None,
            owner_group_name: None,
            owner_user_name_raw: None,
            owner_group_name_raw: None,
            owner_uid: None,
            owner_gid: None,
            segments: value.segments,
        }
    }
}

fn default_host_os_code() -> u64 {
    1
}

fn decode_cached_headers(data: &[u8]) -> Result<CachedArchiveHeaders, rmp_serde::decode::Error> {
    let current_error = match rmp_serde::from_slice::<CachedArchiveHeaders>(data) {
        Ok(cached) => return Ok(cached),
        Err(error) => error,
    };

    match rmp_serde::from_slice::<LegacyCachedArchiveHeaders>(data) {
        Ok(cached) => Ok(cached.into()),
        Err(_) => Err(current_error),
    }
}

fn encode_system_time(time: Option<SystemTime>) -> Option<u64> {
    time.and_then(|value| {
        value
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
    })
}

fn decode_system_time(time_ns: Option<u64>) -> Option<SystemTime> {
    time_ns.and_then(|value| {
        let secs = value / 1_000_000_000;
        let nanos = (value % 1_000_000_000) as u32;
        UNIX_EPOCH.checked_add(Duration::new(secs, nanos))
    })
}

fn encode_redirection_type(redirection: &RedirectionType) -> u64 {
    match redirection {
        RedirectionType::UnixSymlink => 1,
        RedirectionType::WindowsSymlink => 2,
        RedirectionType::WindowsJunction => 3,
        RedirectionType::Hardlink => 4,
        RedirectionType::FileCopy => 5,
        RedirectionType::Unknown(value) => *value,
    }
}

fn encode_host_os(host_os: HostOs) -> u64 {
    match host_os {
        HostOs::Windows => 0,
        HostOs::Unix => 1,
        HostOs::Unknown(value) => value,
    }
}

fn decode_host_os(host_os: u64) -> HostOs {
    match host_os {
        0 => HostOs::Windows,
        1 => HostOs::Unix,
        value => HostOs::Unknown(value),
    }
}

fn cached_segments(segments: &[DataSegment]) -> Vec<CachedSegment> {
    segments
        .iter()
        .map(|s| {
            let (packed_crc32, packed_blake2_hash) = match s.packed_hash {
                Some(PackedDataHash::Crc32(value)) => (Some(value), None),
                Some(PackedDataHash::Blake2sp(value)) => (None, Some(value)),
                None => (None, None),
            };
            CachedSegment {
                volume_index: s.volume_index,
                data_offset: s.data_offset,
                data_size: s.data_size,
                packed_crc32,
                packed_blake2_hash,
                packed_hash_uses_mac: s.packed_hash_uses_mac,
            }
        })
        .collect()
}

fn data_segments(segments: Vec<CachedSegment>) -> Vec<DataSegment> {
    segments
        .into_iter()
        .map(|s| {
            let packed_hash = s
                .packed_blake2_hash
                .map(PackedDataHash::Blake2sp)
                .or_else(|| s.packed_crc32.map(PackedDataHash::Crc32));
            DataSegment::with_packed_hash(
                s.volume_index,
                s.data_offset,
                s.data_size,
                packed_hash,
                s.packed_hash_uses_mac,
            )
        })
        .collect()
}

fn cached_encryption(encryption: &FileEncryptionInfo) -> CachedEncryption {
    CachedEncryption {
        version: encryption.version,
        kdf_count: encryption.kdf_count,
        salt: encryption.salt,
        iv: encryption.iv,
        check_data: encryption.check_data,
        use_hash_mac: encryption.use_hash_mac,
    }
}

fn file_encryption(encryption: CachedEncryption) -> FileEncryptionInfo {
    FileEncryptionInfo {
        version: encryption.version,
        kdf_count: encryption.kdf_count,
        salt: encryption.salt,
        iv: encryption.iv,
        check_data: encryption.check_data,
        use_hash_mac: encryption.use_hash_mac,
    }
}

impl RarArchive {
    /// Export parsed headers for journal persistence.
    pub fn export_headers(&self) -> CachedArchiveHeaders {
        CachedArchiveHeaders {
            format: match self.format {
                ArchiveFormat::Rar14 => 14,
                ArchiveFormat::Rar4 => 4,
                ArchiveFormat::Rar5 => 5,
            },
            is_solid: self.is_solid,
            is_encrypted: self.is_encrypted,
            has_recovery_record: self.has_recovery_record,
            recovery_records: self.recovery_records.clone(),
            is_locked: self.is_locked,
            has_authenticity_verification: self.has_authenticity_verification,
            has_locator: self.has_locator,
            quick_open_offset: self.quick_open_offset,
            recovery_record_offset: self.recovery_record_offset,
            original_name: self.original_name.clone(),
            original_name_raw: self.original_name_raw.clone(),
            original_creation_time_ns: encode_system_time(self.original_creation_time),
            more_volumes: self.more_volumes,
            volume_presence: self.volume_set.presence(),
            last_volume_seen: self.volume_set.last_volume_seen(),
            members: self
                .members
                .iter()
                .map(|m| CachedMember {
                    name: m.file_header.name.clone(),
                    name_raw: m.file_header.name_raw.clone(),
                    unpacked_size: m.file_header.unpacked_size,
                    mtime_ns: encode_system_time(m.file_header.mtime),
                    ctime_ns: encode_system_time(m.file_header.ctime),
                    atime_ns: encode_system_time(m.file_header.atime),
                    data_crc32: m.file_header.data_crc32,
                    compression_method: m.file_header.compression.method.code(),
                    compression_version: m.file_header.compression.version,
                    compression_solid: m.file_header.compression.solid,
                    dict_size: m.file_header.compression.dict_size,
                    split_before: m.file_header.split_before,
                    split_after: m.file_header.split_after,
                    is_directory: m.file_header.is_directory,
                    is_encrypted: m.is_encrypted,
                    encryption: m.file_encryption.as_ref().map(cached_encryption),
                    rar4_salt: m.rar4_salt,
                    version: m.file_header.version,
                    blake2_hash: m.hash.as_ref().map(|h| match h {
                        FileHash::Blake2sp(b) => *b,
                    }),
                    redirection_type: m
                        .redirection
                        .as_ref()
                        .map(|redir| encode_redirection_type(&redir.redir_type)),
                    redirection_target: m.redirection.as_ref().map(|redir| redir.target.clone()),
                    redirection_target_raw: m
                        .redirection
                        .as_ref()
                        .and_then(|redir| redir.target_raw.clone()),
                    redirection_target_is_directory: m
                        .redirection
                        .as_ref()
                        .is_some_and(|redir| redir.target_is_directory),
                    attributes: m.file_header.attributes.0,
                    host_os: encode_host_os(m.file_header.host_os),
                    owner_user_name: m.owner.as_ref().and_then(|owner| owner.user_name.clone()),
                    owner_group_name: m.owner.as_ref().and_then(|owner| owner.group_name.clone()),
                    owner_user_name_raw: m
                        .owner
                        .as_ref()
                        .and_then(|owner| owner.user_name_raw.clone()),
                    owner_group_name_raw: m
                        .owner
                        .as_ref()
                        .and_then(|owner| owner.group_name_raw.clone()),
                    owner_uid: m.owner.as_ref().and_then(|owner| owner.uid),
                    owner_gid: m.owner.as_ref().and_then(|owner| owner.gid),
                    segments: cached_segments(&m.segments),
                })
                .collect(),
            services: self
                .services
                .iter()
                .map(|service| CachedService {
                    header_offset: service.header_offset,
                    name: service.file_header.name.clone(),
                    name_raw: service.file_header.name_raw.clone(),
                    unpacked_size: service.file_header.unpacked_size,
                    mtime_ns: encode_system_time(service.file_header.mtime),
                    ctime_ns: encode_system_time(service.file_header.ctime),
                    atime_ns: encode_system_time(service.file_header.atime),
                    data_crc32: service.file_header.data_crc32,
                    compression_method: service.file_header.compression.method.code(),
                    compression_version: service.file_header.compression.version,
                    compression_solid: service.file_header.compression.solid,
                    dict_size: service.file_header.compression.dict_size,
                    split_before: service.file_header.split_before,
                    split_after: service.file_header.split_after,
                    is_encrypted: service.is_encrypted,
                    encryption: service.file_encryption.as_ref().map(cached_encryption),
                    version: service.file_header.version,
                    blake2_hash: service.hash.as_ref().map(|h| match h {
                        FileHash::Blake2sp(b) => *b,
                    }),
                    comment_crc16: service.comment_crc16,
                    attributes: service.file_header.attributes.0,
                    host_os: encode_host_os(service.file_header.host_os),
                    service_subdata: service.file_header.service_subdata.clone(),
                    segments: cached_segments(&service.segments),
                })
                .collect(),
        }
    }

    /// Reconstruct a `RarArchive` from cached headers (no volume readers).
    ///
    /// The reconstructed archive has no volume readers attached. It can be used
    /// with `extract_member_streaming_chunked` which obtains volumes via a
    /// `VolumeProvider` instead of pre-loaded readers.
    pub fn from_cached_headers(cached: CachedArchiveHeaders) -> Self {
        Self::from_cached_headers_with_password_and_shared_kdf_cache(
            cached,
            None::<String>,
            Arc::new(crate::crypto::KdfCache::new()),
        )
    }

    /// Reconstruct a `RarArchive` from cached headers and optionally restore
    /// the password required for parsing additional encrypted volumes.
    pub fn from_cached_headers_with_password(
        cached: CachedArchiveHeaders,
        password: impl Into<Option<String>>,
    ) -> Self {
        Self::from_cached_headers_with_password_and_shared_kdf_cache(
            cached,
            password,
            Arc::new(crate::crypto::KdfCache::new()),
        )
    }

    pub fn from_cached_headers_with_password_and_shared_kdf_cache(
        cached: CachedArchiveHeaders,
        password: impl Into<Option<String>>,
        kdf_cache: Arc<crate::crypto::KdfCache>,
    ) -> Self {
        let format = match cached.format {
            14 => ArchiveFormat::Rar14,
            4 => ArchiveFormat::Rar4,
            _ => ArchiveFormat::Rar5,
        };

        let members: Vec<MemberEntry> = cached
            .members
            .into_iter()
            .map(|cm| {
                let compression = CompressionInfo {
                    format,
                    version: cm.compression_version,
                    solid: cm.compression_solid,
                    method: CompressionMethod::from_code(cm.compression_method),
                    dict_size: cm.dict_size,
                };

                MemberEntry {
                    file_header: FileHeader {
                        name: cm.name,
                        name_raw: cm.name_raw,
                        unpacked_size: cm.unpacked_size,
                        attributes: FileAttributes(cm.attributes),
                        mtime: decode_system_time(cm.mtime_ns),
                        ctime: decode_system_time(cm.ctime_ns),
                        atime: decode_system_time(cm.atime_ns),
                        data_crc32: cm.data_crc32,
                        data_hash: cm.data_crc32.map(crate::types::DataHash::Crc32),
                        compression,
                        host_os: decode_host_os(cm.host_os),
                        is_directory: cm.is_directory,
                        file_flags: 0,
                        data_size: 0,
                        split_before: cm.split_before,
                        split_after: cm.split_after,
                        data_offset: 0,
                        is_encrypted: cm.is_encrypted,
                        version: cm.version,
                        service_subdata: None,
                    },
                    is_encrypted: cm.is_encrypted,
                    file_encryption: cm.encryption.map(file_encryption),
                    rar4_salt: cm.rar4_salt,
                    hash: cm.blake2_hash.map(FileHash::Blake2sp),
                    redirection: cm.redirection_type.map(|redir_type| Redirection {
                        redir_type: RedirectionType::from(redir_type),
                        target: cm.redirection_target.unwrap_or_default(),
                        target_raw: cm.redirection_target_raw,
                        target_is_directory: cm.redirection_target_is_directory,
                    }),
                    owner: (cm.owner_user_name.is_some()
                        || cm.owner_group_name.is_some()
                        || cm.owner_user_name_raw.is_some()
                        || cm.owner_group_name_raw.is_some()
                        || cm.owner_uid.is_some()
                        || cm.owner_gid.is_some())
                    .then_some(UnixOwnerInfo {
                        user_name: cm.owner_user_name,
                        group_name: cm.owner_group_name,
                        user_name_raw: cm.owner_user_name_raw,
                        group_name_raw: cm.owner_group_name_raw,
                        uid: cm.owner_uid,
                        gid: cm.owner_gid,
                    }),
                    segments: data_segments(cm.segments),
                }
            })
            .collect();
        let services: Vec<ServiceEntry> = cached
            .services
            .into_iter()
            .map(|service| {
                let compression = CompressionInfo {
                    format,
                    version: service.compression_version,
                    solid: service.compression_solid,
                    method: CompressionMethod::from_code(service.compression_method),
                    dict_size: service.dict_size,
                };

                ServiceEntry {
                    header_offset: service.header_offset,
                    file_header: FileHeader {
                        name: service.name,
                        name_raw: service.name_raw,
                        unpacked_size: service.unpacked_size,
                        attributes: FileAttributes(service.attributes),
                        mtime: decode_system_time(service.mtime_ns),
                        ctime: decode_system_time(service.ctime_ns),
                        atime: decode_system_time(service.atime_ns),
                        data_crc32: service.data_crc32,
                        data_hash: service.data_crc32.map(crate::types::DataHash::Crc32),
                        compression,
                        host_os: decode_host_os(service.host_os),
                        is_directory: false,
                        file_flags: 0,
                        data_size: 0,
                        split_before: service.split_before,
                        split_after: service.split_after,
                        data_offset: 0,
                        is_encrypted: service.is_encrypted,
                        version: service.version,
                        service_subdata: service.service_subdata,
                    },
                    is_encrypted: service.is_encrypted,
                    file_encryption: service.encryption.map(file_encryption),
                    hash: service.blake2_hash.map(FileHash::Blake2sp),
                    comment_crc16: service.comment_crc16,
                    segments: data_segments(service.segments),
                }
            })
            .collect();
        let mut archive = RarArchive {
            format,
            is_solid: cached.is_solid,
            is_encrypted: cached.is_encrypted,
            has_recovery_record: cached.has_recovery_record,
            recovery_records: cached.recovery_records,
            is_locked: cached.is_locked,
            has_authenticity_verification: cached.has_authenticity_verification,
            has_locator: cached.has_locator,
            quick_open_offset: cached.quick_open_offset,
            recovery_record_offset: cached.recovery_record_offset,
            original_name: cached.original_name,
            original_name_raw: cached.original_name_raw,
            original_creation_time: decode_system_time(cached.original_creation_time_ns),
            volume_set: VolumeSet::from_presence(cached.volume_presence, cached.last_volume_seen),
            members,
            services,
            more_volumes: cached.more_volumes,
            volumes: Vec::new(),
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: password.into(),
            kdf_cache,
        };
        archive.sort_members_by_physical_order();
        archive
    }

    /// Serialize headers to MessagePack bytes.
    pub fn serialize_headers(&self) -> Vec<u8> {
        let cached = self.export_headers();
        rmp_serde::to_vec_named(&cached).expect("header serialization should not fail")
    }

    /// Deserialize headers from MessagePack bytes and reconstruct archive.
    pub fn deserialize_headers(data: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        Self::deserialize_headers_with_password(data, None::<String>)
    }

    /// Deserialize headers from MessagePack bytes and restore a password for
    /// subsequent encrypted volume parsing if needed.
    pub fn deserialize_headers_with_password(
        data: &[u8],
        password: impl Into<Option<String>>,
    ) -> Result<Self, rmp_serde::decode::Error> {
        Self::deserialize_headers_with_password_and_shared_kdf_cache(
            data,
            password,
            Arc::new(crate::crypto::KdfCache::new()),
        )
    }

    pub fn deserialize_headers_with_password_and_shared_kdf_cache(
        data: &[u8],
        password: impl Into<Option<String>>,
        kdf_cache: Arc<crate::crypto::KdfCache>,
    ) -> Result<Self, rmp_serde::decode::Error> {
        let cached = decode_cached_headers(data)?;
        Ok(
            Self::from_cached_headers_with_password_and_shared_kdf_cache(
                cached, password, kdf_cache,
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    struct OldCachedArchiveHeaders {
        format: u8,
        is_solid: bool,
        is_encrypted: bool,
        more_volumes: bool,
        volume_presence: Vec<bool>,
        last_volume_seen: bool,
        members: Vec<OldCachedMember>,
    }

    #[derive(Serialize)]
    struct PriorNamedCachedArchiveHeaders {
        format: u8,
        is_solid: bool,
        is_encrypted: bool,
        has_recovery_record: bool,
        recovery_records: Vec<RecoveryRecordInfo>,
        is_locked: bool,
        has_authenticity_verification: bool,
        has_locator: bool,
        quick_open_offset: Option<u64>,
        recovery_record_offset: Option<u64>,
        original_name: Option<String>,
        original_creation_time_ns: Option<u64>,
        more_volumes: bool,
        volume_presence: Vec<bool>,
        last_volume_seen: bool,
        members: Vec<CachedMember>,
    }

    #[derive(Serialize)]
    struct OldCachedMember {
        name: String,
        unpacked_size: Option<u64>,
        data_crc32: Option<u32>,
        compression_method: u8,
        compression_version: u8,
        compression_solid: bool,
        dict_size: u64,
        split_before: bool,
        split_after: bool,
        is_directory: bool,
        is_encrypted: bool,
        encryption: Option<CachedEncryption>,
        rar4_salt: Option<[u8; 8]>,
        blake2_hash: Option<[u8; 32]>,
        segments: Vec<CachedSegment>,
    }

    #[test]
    fn deserialize_headers_accepts_legacy_compact_cache() {
        let blake2_hash = [0x42; 32];
        let cached = OldCachedArchiveHeaders {
            format: 5,
            is_solid: true,
            is_encrypted: true,
            more_volumes: true,
            volume_presence: vec![true, false],
            last_volume_seen: false,
            members: vec![OldCachedMember {
                name: "episode.mkv".to_string(),
                unpacked_size: Some(4096),
                data_crc32: Some(0x1234_5678),
                compression_method: CompressionMethod::Normal.code(),
                compression_version: 1,
                compression_solid: true,
                dict_size: 8 * 1024 * 1024,
                split_before: false,
                split_after: true,
                is_directory: false,
                is_encrypted: true,
                encryption: Some(CachedEncryption {
                    version: 0,
                    kdf_count: 15,
                    salt: [1; 16],
                    iv: [2; 16],
                    check_data: Some([3; 12]),
                    use_hash_mac: true,
                }),
                rar4_salt: None,
                blake2_hash: Some(blake2_hash),
                segments: vec![CachedSegment {
                    volume_index: 0,
                    data_offset: 128,
                    data_size: 256,
                    packed_crc32: None,
                    packed_blake2_hash: None,
                    packed_hash_uses_mac: false,
                }],
            }],
        };
        let bytes = rmp_serde::to_vec(&cached).expect("legacy compact cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("legacy cache should decode");

        let member = &archive.members[0];
        assert_eq!(member.file_header.compression.version, 1);
        assert_eq!(member.hash, Some(FileHash::Blake2sp(blake2_hash)));
        assert!(
            member
                .file_encryption
                .as_ref()
                .is_some_and(|enc| enc.use_hash_mac)
        );
        assert!(member.file_header.mtime.is_none());
        assert!(member.redirection.is_none());
    }

    #[test]
    fn cached_headers_preserve_recovery_record_metadata() {
        let record = RecoveryRecordInfo {
            format: ArchiveFormat::Rar5,
            kind: crate::types::RecoveryRecordKind::Rar5Service,
            offset: 1024,
            data_offset: 2048,
            data_size: 4096,
            protected_size: Some(1024),
            recovery_sectors: None,
            total_blocks: None,
            recovery_percent: Some(7),
        };
        let cached = CachedArchiveHeaders {
            format: 5,
            is_solid: false,
            is_encrypted: false,
            has_recovery_record: true,
            recovery_records: vec![record.clone()],
            is_locked: false,
            has_authenticity_verification: false,
            has_locator: false,
            quick_open_offset: None,
            recovery_record_offset: None,
            original_name: None,
            original_name_raw: None,
            original_creation_time_ns: None,
            more_volumes: false,
            volume_presence: vec![true],
            last_volume_seen: true,
            members: Vec::new(),
            services: Vec::new(),
        };
        let bytes = rmp_serde::to_vec(&cached).expect("cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("cache should decode");
        let metadata = archive.metadata();

        assert!(metadata.has_recovery_record);
        assert_eq!(metadata.recovery_records, vec![record]);
    }

    #[test]
    fn deserialize_headers_accepts_named_cache_without_services() {
        let cached = PriorNamedCachedArchiveHeaders {
            format: 5,
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
            original_creation_time_ns: None,
            more_volumes: false,
            volume_presence: vec![true],
            last_volume_seen: true,
            members: Vec::new(),
        };
        let bytes = rmp_serde::to_vec_named(&cached).expect("prior cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("prior cache should decode");

        assert!(archive.services.is_empty());
    }

    #[test]
    fn cached_headers_preserve_service_entries() {
        let blake2_hash = [0x5a; 32];
        let cached = CachedArchiveHeaders {
            format: 5,
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
            original_creation_time_ns: None,
            more_volumes: false,
            volume_presence: vec![true],
            last_volume_seen: true,
            members: Vec::new(),
            services: vec![CachedService {
                header_offset: 0x1234,
                name: "CMT".to_string(),
                name_raw: Some(b"CMT".to_vec()),
                unpacked_size: Some(12),
                mtime_ns: None,
                ctime_ns: None,
                atime_ns: None,
                data_crc32: Some(0xA1B2_C3D4),
                compression_method: CompressionMethod::Store.code(),
                compression_version: 5,
                compression_solid: false,
                dict_size: 0,
                split_before: false,
                split_after: true,
                is_encrypted: true,
                encryption: Some(CachedEncryption {
                    version: 0,
                    kdf_count: 12,
                    salt: [0x11; 16],
                    iv: [0x22; 16],
                    check_data: Some([0x33; 12]),
                    use_hash_mac: true,
                }),
                version: Some(7),
                blake2_hash: Some(blake2_hash),
                comment_crc16: Some(0x4567),
                attributes: 0x20,
                host_os: encode_host_os(HostOs::Unix),
                service_subdata: Some(vec![1, 2, 3]),
                segments: vec![CachedSegment {
                    volume_index: 3,
                    data_offset: 4096,
                    data_size: 8192,
                    packed_crc32: Some(0x0102_0304),
                    packed_blake2_hash: None,
                    packed_hash_uses_mac: true,
                }],
            }],
        };
        let bytes = rmp_serde::to_vec_named(&cached).expect("cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("cache should decode");
        let service = &archive.services[0];

        assert_eq!(service.header_offset, 0x1234);
        assert_eq!(service.file_header.name, "CMT");
        assert_eq!(
            service.file_header.name_raw.as_deref(),
            Some(&b"CMT"[..])
        );
        assert_eq!(service.file_header.unpacked_size, Some(12));
        assert_eq!(service.file_header.data_crc32, Some(0xA1B2_C3D4));
        assert_eq!(
            service.file_header.compression.method,
            CompressionMethod::Store
        );
        assert_eq!(service.file_header.version, Some(7));
        assert_eq!(
            service.file_header.service_subdata.as_deref(),
            Some(&[1, 2, 3][..])
        );
        assert_eq!(service.hash, Some(FileHash::Blake2sp(blake2_hash)));
        assert_eq!(service.comment_crc16, Some(0x4567));
        assert!(service.is_encrypted);
        assert!(
            service
                .file_encryption
                .as_ref()
                .is_some_and(|enc| enc.use_hash_mac)
        );
        assert_eq!(service.segments.len(), 1);
        assert_eq!(service.segments[0].volume_index, 3);
        assert_eq!(
            service.segments[0].packed_hash,
            Some(PackedDataHash::Crc32(0x0102_0304))
        );
        assert!(service.segments[0].packed_hash_uses_mac);
    }

    #[test]
    fn cached_headers_preserve_raw_unix_owner_names() {
        let cached = CachedArchiveHeaders {
            format: 5,
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
            original_creation_time_ns: None,
            more_volumes: false,
            volume_presence: vec![true],
            last_volume_seen: true,
            members: vec![CachedMember {
                name: "file.txt".to_string(),
                name_raw: Some(b"file.txt".to_vec()),
                unpacked_size: Some(0),
                mtime_ns: None,
                ctime_ns: None,
                atime_ns: None,
                data_crc32: None,
                compression_method: CompressionMethod::Store.code(),
                compression_version: 5,
                compression_solid: false,
                dict_size: 0,
                split_before: false,
                split_after: false,
                is_directory: false,
                is_encrypted: false,
                encryption: None,
                rar4_salt: None,
                version: None,
                blake2_hash: None,
                redirection_type: Some(1),
                redirection_target: Some("a\u{fffd}b".to_string()),
                redirection_target_raw: Some(b"a\xed\xa0\x80b".to_vec()),
                redirection_target_is_directory: false,
                attributes: 0,
                host_os: default_host_os_code(),
                owner_user_name: Some("al\u{fffd}ice".to_string()),
                owner_group_name: Some("gr\u{fffd}up".to_string()),
                owner_user_name_raw: Some(b"al\xffice".to_vec()),
                owner_group_name_raw: Some(b"gr\xf0up".to_vec()),
                owner_uid: None,
                owner_gid: None,
                segments: Vec::new(),
            }],
            services: Vec::new(),
        };
        let bytes = rmp_serde::to_vec_named(&cached).expect("cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("cache should decode");
        let owner = archive.members[0].owner.as_ref().expect("owner restored");

        assert_eq!(
            archive.members[0].file_header.name_raw.as_deref(),
            Some(&b"file.txt"[..])
        );
        assert_eq!(owner.user_name_raw.as_deref(), Some(&b"al\xffice"[..]));
        assert_eq!(owner.group_name_raw.as_deref(), Some(&b"gr\xf0up"[..]));
        let redirection = archive.members[0]
            .redirection
            .as_ref()
            .expect("redirection restored");
        assert_eq!(redirection.target, "a\u{fffd}b");
        assert_eq!(
            redirection.target_raw.as_deref(),
            Some(&b"a\xed\xa0\x80b"[..])
        );
    }

    #[test]
    fn cached_headers_preserve_main_header_locator_and_metadata() {
        let ctime = UNIX_EPOCH + Duration::new(1_700_000_123, 456_789_000);
        let cached = CachedArchiveHeaders {
            format: 5,
            is_solid: false,
            is_encrypted: false,
            has_recovery_record: true,
            recovery_records: Vec::new(),
            is_locked: false,
            has_authenticity_verification: false,
            has_locator: true,
            quick_open_offset: Some(4096),
            recovery_record_offset: Some(8192),
            original_name: Some("release.rar".to_string()),
            original_name_raw: Some(b"release.rar".to_vec()),
            original_creation_time_ns: encode_system_time(Some(ctime)),
            more_volumes: false,
            volume_presence: vec![true],
            last_volume_seen: true,
            members: Vec::new(),
            services: Vec::new(),
        };
        let bytes = rmp_serde::to_vec_named(&cached).expect("cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("cache should decode");
        let metadata = archive.metadata();

        assert!(metadata.has_locator);
        assert_eq!(metadata.quick_open_offset, Some(4096));
        assert_eq!(metadata.recovery_record_offset, Some(8192));
        assert_eq!(metadata.original_name.as_deref(), Some("release.rar"));
        assert_eq!(
            metadata.original_name_bytes.as_deref(),
            Some(&b"release.rar"[..])
        );
        assert_eq!(metadata.original_creation_time, Some(ctime));
    }
}
