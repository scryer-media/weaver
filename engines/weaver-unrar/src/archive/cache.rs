//! Serializable archive header cache for journal persistence.
//!
//! Enables reconstructing a `RarArchive` without reading volume 0 from disk.
//! Headers are serialized as named MessagePack maps so added metadata fields
//! remain backward-compatible.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{DataSegment, FileEncryptionInfo, MemberEntry, RarArchive};
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
    pub original_creation_time_ns: Option<u64>,
    pub more_volumes: bool,
    #[serde(default)]
    pub volume_presence: Vec<bool>,
    #[serde(default)]
    pub last_volume_seen: bool,
    pub members: Vec<CachedMember>,
}

#[derive(Serialize, Deserialize)]
pub struct CachedMember {
    pub name: String,
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
    pub owner_uid: Option<u64>,
    #[serde(default)]
    pub owner_gid: Option<u64>,
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
            original_creation_time_ns: None,
            more_volumes: value.more_volumes,
            volume_presence: value.volume_presence,
            last_volume_seen: value.last_volume_seen,
            recovery_records: Vec::new(),
            members: value.members.into_iter().map(CachedMember::from).collect(),
        }
    }
}

impl From<LegacyCachedMember> for CachedMember {
    fn from(value: LegacyCachedMember) -> Self {
        Self {
            name: value.name,
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
            redirection_target_is_directory: false,
            attributes: 0,
            host_os: default_host_os_code(),
            owner_user_name: None,
            owner_group_name: None,
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
            original_creation_time_ns: encode_system_time(self.original_creation_time),
            more_volumes: self.more_volumes,
            volume_presence: self.volume_set.presence(),
            last_volume_seen: self.volume_set.last_volume_seen(),
            members: self
                .members
                .iter()
                .map(|m| CachedMember {
                    name: m.file_header.name.clone(),
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
                    encryption: m.file_encryption.as_ref().map(|e| CachedEncryption {
                        version: e.version,
                        kdf_count: e.kdf_count,
                        salt: e.salt,
                        iv: e.iv,
                        check_data: e.check_data,
                        use_hash_mac: e.use_hash_mac,
                    }),
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
                    redirection_target_is_directory: m
                        .redirection
                        .as_ref()
                        .is_some_and(|redir| redir.target_is_directory),
                    attributes: m.file_header.attributes.0,
                    host_os: encode_host_os(m.file_header.host_os),
                    owner_user_name: m.owner.as_ref().and_then(|owner| owner.user_name.clone()),
                    owner_group_name: m.owner.as_ref().and_then(|owner| owner.group_name.clone()),
                    owner_uid: m.owner.as_ref().and_then(|owner| owner.uid),
                    owner_gid: m.owner.as_ref().and_then(|owner| owner.gid),
                    segments: m
                        .segments
                        .iter()
                        .map(|s| CachedSegment {
                            volume_index: s.volume_index,
                            data_offset: s.data_offset,
                            data_size: s.data_size,
                        })
                        .collect(),
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
                    file_encryption: cm.encryption.map(|e| FileEncryptionInfo {
                        version: e.version,
                        kdf_count: e.kdf_count,
                        salt: e.salt,
                        iv: e.iv,
                        check_data: e.check_data,
                        use_hash_mac: e.use_hash_mac,
                    }),
                    rar4_salt: cm.rar4_salt,
                    hash: cm.blake2_hash.map(FileHash::Blake2sp),
                    redirection: cm.redirection_type.map(|redir_type| Redirection {
                        redir_type: RedirectionType::from(redir_type),
                        target: cm.redirection_target.unwrap_or_default(),
                        target_is_directory: cm.redirection_target_is_directory,
                    }),
                    owner: (cm.owner_user_name.is_some()
                        || cm.owner_group_name.is_some()
                        || cm.owner_uid.is_some()
                        || cm.owner_gid.is_some())
                    .then_some(UnixOwnerInfo {
                        user_name: cm.owner_user_name,
                        group_name: cm.owner_group_name,
                        uid: cm.owner_uid,
                        gid: cm.owner_gid,
                    }),
                    segments: cm
                        .segments
                        .into_iter()
                        .map(|s| DataSegment {
                            volume_index: s.volume_index,
                            data_offset: s.data_offset,
                            data_size: s.data_size,
                        })
                        .collect(),
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
            original_creation_time: decode_system_time(cached.original_creation_time_ns),
            volume_set: VolumeSet::from_presence(cached.volume_presence, cached.last_volume_seen),
            members,
            services: Vec::new(),
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
            original_creation_time_ns: None,
            more_volumes: false,
            volume_presence: vec![true],
            last_volume_seen: true,
            members: Vec::new(),
        };
        let bytes = rmp_serde::to_vec(&cached).expect("cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("cache should decode");
        let metadata = archive.metadata();

        assert!(metadata.has_recovery_record);
        assert_eq!(metadata.recovery_records, vec![record]);
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
            original_creation_time_ns: encode_system_time(Some(ctime)),
            more_volumes: false,
            volume_presence: vec![true],
            last_volume_seen: true,
            members: Vec::new(),
        };
        let bytes = rmp_serde::to_vec_named(&cached).expect("cache should serialize");

        let archive = RarArchive::deserialize_headers(&bytes).expect("cache should decode");
        let metadata = archive.metadata();

        assert!(metadata.has_locator);
        assert_eq!(metadata.quick_open_offset, Some(4096));
        assert_eq!(metadata.recovery_record_offset, Some(8192));
        assert_eq!(metadata.original_name.as_deref(), Some("release.rar"));
        assert_eq!(metadata.original_creation_time, Some(ctime));
    }
}
