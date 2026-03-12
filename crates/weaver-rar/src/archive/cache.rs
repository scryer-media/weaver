//! Serializable archive header cache for journal persistence.
//!
//! Enables reconstructing a `RarArchive` without reading volume 0 from disk.
//! Headers are serialized with MessagePack (`rmp-serde`) for compactness.

use serde::{Deserialize, Serialize};

use super::{DataSegment, FileEncryptionInfo, MemberEntry, RarArchive};
use crate::header::file::FileHeader;
use crate::limits::Limits;
use crate::types::{ArchiveFormat, CompressionInfo, CompressionMethod, FileHash};
use crate::volume::VolumeSet;

/// Serializable snapshot of parsed archive headers.
#[derive(Serialize, Deserialize)]
pub struct CachedArchiveHeaders {
    pub format: u8, // 4=Rar4, 5=Rar5
    pub is_solid: bool,
    pub is_encrypted: bool,
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
    pub data_crc32: Option<u32>,
    pub compression_method: u8,
    pub compression_version: u8,
    pub compression_solid: bool,
    pub dict_size: u64,
    pub split_before: bool,
    pub split_after: bool,
    pub is_directory: bool,
    pub is_encrypted: bool,
    pub encryption: Option<CachedEncryption>,
    pub rar4_salt: Option<[u8; 8]>,
    pub blake2_hash: Option<[u8; 32]>,
    pub segments: Vec<CachedSegment>,
}

#[derive(Serialize, Deserialize)]
pub struct CachedEncryption {
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

impl RarArchive {
    /// Export parsed headers for journal persistence.
    pub fn export_headers(&self) -> CachedArchiveHeaders {
        CachedArchiveHeaders {
            format: match self.format {
                ArchiveFormat::Rar4 => 4,
                ArchiveFormat::Rar5 => 5,
            },
            is_solid: self.is_solid,
            is_encrypted: self.is_encrypted,
            more_volumes: self.more_volumes,
            volume_presence: self.volume_set.presence(),
            last_volume_seen: self.volume_set.last_volume_seen(),
            members: self
                .members
                .iter()
                .map(|m| CachedMember {
                    name: m.file_header.name.clone(),
                    unpacked_size: m.file_header.unpacked_size,
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
                        kdf_count: e.kdf_count,
                        salt: e.salt,
                        iv: e.iv,
                        check_data: e.check_data,
                        use_hash_mac: e.use_hash_mac,
                    }),
                    rar4_salt: m.rar4_salt,
                    blake2_hash: m.hash.as_ref().map(|h| match h {
                        FileHash::Blake2sp(b) => *b,
                    }),
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
        Self::from_cached_headers_with_password(cached, None::<String>)
    }

    /// Reconstruct a `RarArchive` from cached headers and optionally restore
    /// the password required for parsing additional encrypted volumes.
    pub fn from_cached_headers_with_password(
        cached: CachedArchiveHeaders,
        password: impl Into<Option<String>>,
    ) -> Self {
        let format = match cached.format {
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
                        attributes: crate::types::FileAttributes(0),
                        mtime: None,
                        data_crc32: cm.data_crc32,
                        compression,
                        host_os: crate::types::HostOs::Unix,
                        is_directory: cm.is_directory,
                        file_flags: 0,
                        data_size: 0,
                        split_before: cm.split_before,
                        split_after: cm.split_after,
                        data_offset: 0,
                        is_encrypted: cm.is_encrypted,
                    },
                    is_encrypted: cm.is_encrypted,
                    file_encryption: cm.encryption.map(|e| FileEncryptionInfo {
                        kdf_count: e.kdf_count,
                        salt: e.salt,
                        iv: e.iv,
                        check_data: e.check_data,
                        use_hash_mac: e.use_hash_mac,
                    }),
                    rar4_salt: cm.rar4_salt,
                    hash: cm.blake2_hash.map(FileHash::Blake2sp),
                    redirection: None,
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

        RarArchive {
            format,
            is_solid: cached.is_solid,
            is_encrypted: cached.is_encrypted,
            volume_set: VolumeSet::from_presence(cached.volume_presence, cached.last_volume_seen),
            members,
            more_volumes: cached.more_volumes,
            volumes: Vec::new(),
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: password.into(),
        }
    }

    /// Serialize headers to MessagePack bytes.
    pub fn serialize_headers(&self) -> Vec<u8> {
        let cached = self.export_headers();
        rmp_serde::to_vec(&cached).expect("header serialization should not fail")
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
        let cached: CachedArchiveHeaders = rmp_serde::from_slice(data)?;
        Ok(Self::from_cached_headers_with_password(cached, password))
    }
}
