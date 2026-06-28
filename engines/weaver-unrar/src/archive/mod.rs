//! RarArchive: open, parse headers, list members, metadata.
//!
//! Supports single-volume and multi-volume archives. For multi-volume
//! archives, volumes can be added incrementally as they become available.

mod cache;
mod facts;
mod member;
mod parse;
mod volume;

pub use cache::CachedArchiveHeaders;
pub use facts::{RarVolumeFacts, RarVolumeMemberFacts, RarVolumeServiceFacts};

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::SystemTime;

use crate::decompress::lz::LzDecoder;
use crate::decompress::rar4_old::Rar4Decoder;
use crate::error::{RarError, RarResult};
use crate::extract::ExtractOptions;
use crate::header;
use crate::header::file::FileHeader;
use crate::limits::Limits;
use crate::progress::ProgressHandler;
use crate::signature;
use crate::types::{
    ArchiveFormat, ArchiveMetadata, CompressionMethod, FileHash, MemberInfo, RecoveryRecordInfo,
    RecoveryRecordKind, TopologyMemberInfo, UnixOwnerInfo, VolumeSpan,
};
use crate::volume::VolumeSet;

/// File-level encryption parameters extracted from the file's extra records.
#[derive(Debug, Clone)]
pub(super) struct FileEncryptionInfo {
    pub(super) version: u64,
    pub(super) kdf_count: u8,
    pub(super) salt: [u8; 16],
    pub(super) iv: [u8; 16],
    pub(super) check_data: Option<[u8; 12]>,
    /// When true, CRC32/BLAKE2 hashes are HMAC-transformed (enc_flags & 0x0002).
    pub(super) use_hash_mac: bool,
}

/// Internal entry tracking a member and its data segments across volumes.
#[derive(Debug, Clone)]
pub(super) struct MemberEntry {
    /// The parsed file header (from the first volume containing this member).
    pub(super) file_header: FileHeader,
    /// Whether this member is encrypted (file-level encryption).
    pub(super) is_encrypted: bool,
    /// File-level encryption info, if encrypted (RAR5).
    pub(super) file_encryption: Option<FileEncryptionInfo>,
    /// RAR4 encryption salt (8 bytes), if encrypted.
    pub(super) rar4_salt: Option<[u8; 8]>,
    /// Optional BLAKE2sp hash from extra records.
    pub(super) hash: Option<FileHash>,
    /// Redirection info (symlink/hardlink) from extra records.
    pub(super) redirection: Option<header::Redirection>,
    /// Unix owner info from the RAR5 owner extra field.
    pub(super) owner: Option<UnixOwnerInfo>,
    /// Data segments across volumes, in order.
    pub(super) segments: Vec<DataSegment>,
}

/// Internal entry tracking a RAR5 service header and its subdata.
#[derive(Debug, Clone)]
pub(super) struct ServiceEntry {
    pub(super) header_offset: u64,
    pub(super) file_header: FileHeader,
    pub(super) is_encrypted: bool,
    pub(super) file_encryption: Option<FileEncryptionInfo>,
    pub(super) hash: Option<FileHash>,
    pub(super) comment_crc16: Option<u16>,
    pub(super) segments: Vec<DataSegment>,
}

/// Integrity hash for a split segment's packed bytes.
///
/// UnRAR treats hashes on split-after headers as `Pack-CRC32`/`Pack-BLAKE2`;
/// only the final continuation's hash is the whole unpacked file hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PackedDataHash {
    Crc32(u32),
    Blake2sp([u8; 32]),
}

/// A contiguous data segment within a single volume.
#[derive(Debug, Clone)]
pub struct DataSegment {
    /// Volume index this segment lives in.
    pub volume_index: usize,
    /// Offset within the volume reader where data begins.
    pub data_offset: u64,
    /// Size of this data segment.
    pub data_size: u64,
    /// Optional hash of this segment's packed bytes, present on split-after
    /// headers and verified as we leave the segment.
    pub(super) packed_hash: Option<PackedDataHash>,
    pub(super) packed_hash_uses_mac: bool,
}

impl DataSegment {
    pub(super) fn new(volume_index: usize, data_offset: u64, data_size: u64) -> Self {
        Self {
            volume_index,
            data_offset,
            data_size,
            packed_hash: None,
            packed_hash_uses_mac: false,
        }
    }

    pub(super) fn with_packed_hash(
        volume_index: usize,
        data_offset: u64,
        data_size: u64,
        packed_hash: Option<PackedDataHash>,
        packed_hash_uses_mac: bool,
    ) -> Self {
        Self {
            volume_index,
            data_offset,
            data_size,
            packed_hash,
            packed_hash_uses_mac: packed_hash.is_some() && packed_hash_uses_mac,
        }
    }
}

impl RarArchive {
    pub(super) fn packed_hash_for_split_segment(
        file_header: &FileHeader,
        hash: Option<&FileHash>,
    ) -> Option<PackedDataHash> {
        if !file_header.split_after {
            return None;
        }

        if file_header.compression.format == ArchiveFormat::Rar5 {
            return hash
                .map(|hash| match hash {
                    FileHash::Blake2sp(value) => PackedDataHash::Blake2sp(*value),
                })
                .or_else(|| file_header.data_crc32.map(PackedDataHash::Crc32));
        }

        if file_header.compression.format.is_rar4_family() && file_header.compression.version >= 20
        {
            return file_header.data_crc32.map(PackedDataHash::Crc32);
        }

        None
    }
}

/// Holder for a volume's reader and parsed metadata.
pub(super) struct VolumeData {
    pub(super) reader: Box<dyn ReadSeek>,
}

/// Trait alias for Read + Seek so we can use trait objects.
pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

/// A parsed RAR5 archive that can list members and extract files.
///
/// Supports both single-volume and multi-volume archives. For multi-volume
/// archives, use `open()` to parse the first volume, then `add_volume()`
/// to register additional volumes as they become available.
pub struct RarArchive {
    /// Parsed archive metadata.
    pub(super) format: ArchiveFormat,
    /// Whether the archive is solid.
    pub(super) is_solid: bool,
    /// Whether the archive has header-level encryption.
    pub(super) is_encrypted: bool,
    /// Whether the archive has a recovery record.
    pub(super) has_recovery_record: bool,
    /// Parsed embedded recovery records.
    pub(super) recovery_records: Vec<crate::types::RecoveryRecordInfo>,
    /// Whether the archive is locked.
    pub(super) is_locked: bool,
    /// Whether the archive has authenticity verification/auth info.
    pub(super) has_authenticity_verification: bool,
    /// Whether a RAR5 main-header locator extra was present.
    pub(super) has_locator: bool,
    /// Absolute quick-open block offset from the RAR5 main-header locator.
    pub(super) quick_open_offset: Option<u64>,
    /// Absolute embedded recovery-record offset from the RAR5 main-header locator.
    pub(super) recovery_record_offset: Option<u64>,
    /// Original archive name from the RAR5 main-header metadata extra.
    pub(super) original_name: Option<String>,
    /// Raw original archive name bytes from the RAR5 main-header metadata extra.
    pub(super) original_name_raw: Option<Vec<u8>>,
    /// Original archive creation time from the RAR5 main-header metadata extra.
    pub(super) original_creation_time: Option<SystemTime>,
    /// Volume topology.
    pub(super) volume_set: VolumeSet,
    /// All member entries (file headers + data segments).
    pub(super) members: Vec<MemberEntry>,
    /// RAR5 service entries, such as comments and recovery records.
    pub(super) services: Vec<ServiceEntry>,
    /// Whether more volumes follow.
    pub(super) more_volumes: bool,
    /// Volume readers, indexed by volume number.
    pub(super) volumes: Vec<Option<VolumeData>>,
    /// RAR5 LZ decoder state for solid archive extraction (carries across members).
    pub(super) solid_decoder: Option<LzDecoder>,
    /// RAR4 decoder state for solid archive extraction.
    pub(super) solid_decoder_rar4: Option<Rar4Decoder>,
    /// Index of the next member that must be extracted in a solid archive.
    pub(super) solid_next_index: usize,
    /// Resource limits for archive processing.
    pub(super) limits: Limits,
    /// Password for decrypting encrypted archives/members.
    pub(super) password: Option<String>,
    /// Cache for expensive key derivation (shared across member extractions).
    pub(super) kdf_cache: Arc<crate::crypto::KdfCache>,
}

#[derive(Debug, Clone)]
pub struct MemberPlannerState {
    pub name: String,
    pub extractable: bool,
    pub missing_volumes: Vec<usize>,
}

impl RarArchive {
    /// Open and parse a RAR archive from a reader.
    pub fn open(reader: impl Read + Seek + Send + 'static) -> RarResult<Self> {
        Self::open_with_shared_kdf_cache(reader, Arc::new(crate::crypto::KdfCache::new()))
    }

    pub fn open_with_shared_kdf_cache(
        reader: impl Read + Seek + Send + 'static,
        kdf_cache: Arc<crate::crypto::KdfCache>,
    ) -> RarResult<Self> {
        let reader: Box<dyn ReadSeek> = Box::new(reader);
        Self::open_boxed_inner(reader, None, kdf_cache)
    }

    /// Open and parse an encrypted RAR archive with a password.
    ///
    /// The password is used both for decrypting archive-level headers and for
    /// decrypting individual encrypted members during extraction.
    pub fn open_with_password(
        reader: impl Read + Seek + Send + 'static,
        password: &str,
    ) -> RarResult<Self> {
        Self::open_with_password_and_shared_kdf_cache(
            reader,
            password,
            Arc::new(crate::crypto::KdfCache::new()),
        )
    }

    pub fn open_with_password_and_shared_kdf_cache(
        reader: impl Read + Seek + Send + 'static,
        password: &str,
        kdf_cache: Arc<crate::crypto::KdfCache>,
    ) -> RarResult<Self> {
        let reader: Box<dyn ReadSeek> = Box::new(reader);
        Self::open_boxed_inner(reader, Some(password), kdf_cache)
    }

    /// Set or update the password for decrypting encrypted members.
    ///
    /// This does not re-parse headers; it only affects subsequent extraction
    /// calls. For archive-level encryption (encrypted headers), use
    /// `open_with_password()` instead.
    pub fn set_password(&mut self, password: impl Into<String>) {
        self.password = Some(password.into());
    }

    /// Set resource limits for archive processing.
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Check if a member is extractable (all required volumes are present).
    ///
    /// A member is extractable when:
    /// 1. All known data segments have their volume readers present, AND
    /// 2. The member is not still waiting for continuation segments
    ///    (i.e. the last segment does not have `split_after` set, or
    ///    equivalently, a segment from the volume that terminates this
    ///    member has been registered).
    pub fn is_extractable(&self, name: &str) -> bool {
        if let Some(entry) = self.members.iter().find(|m| m.file_header.name == name) {
            // If the file header still has split_after, we haven't seen the
            // final continuation segment yet.
            if entry.file_header.split_after {
                return false;
            }
            // Check that we have readers for all segments' volumes.
            entry.segments.iter().all(|seg| {
                self.volumes
                    .get(seg.volume_index)
                    .is_some_and(|v| v.is_some())
            })
        } else {
            false
        }
    }

    /// List which volume indices are still needed for a member.
    ///
    /// This includes both volumes whose readers are missing AND the next
    /// expected volume if the member still has continuation segments pending.
    pub fn missing_volumes(&self, name: &str) -> Vec<usize> {
        if let Some(entry) = self.members.iter().find(|m| m.file_header.name == name) {
            let mut missing: Vec<usize> = entry
                .segments
                .iter()
                .filter(|seg| {
                    !self
                        .volumes
                        .get(seg.volume_index)
                        .is_some_and(|v| v.is_some())
                })
                .map(|seg| seg.volume_index)
                .collect();

            // If the member is still split (awaiting more segments),
            // report the next volume after the last known segment.
            if entry.file_header.split_after {
                let next_vol = entry
                    .segments
                    .last()
                    .map(|s| s.volume_index + 1)
                    .unwrap_or(1);
                if !missing.contains(&next_vol) {
                    missing.push(next_vol);
                }
            }

            missing
        } else {
            Vec::new()
        }
    }

    /// Planner-oriented member state, keyed by sanitized member name.
    pub fn planner_member_states(&self) -> Vec<MemberPlannerState> {
        self.members
            .iter()
            .map(|entry| {
                let extractable = !entry.file_header.split_after
                    && entry.segments.iter().all(|seg| {
                        self.volumes
                            .get(seg.volume_index)
                            .is_some_and(|volume| volume.is_some())
                    });

                let mut missing_volumes: Vec<usize> = entry
                    .segments
                    .iter()
                    .filter(|seg| {
                        !self
                            .volumes
                            .get(seg.volume_index)
                            .is_some_and(|volume| volume.is_some())
                    })
                    .map(|seg| seg.volume_index)
                    .collect();

                if entry.file_header.split_after {
                    let next_vol = entry
                        .segments
                        .last()
                        .map(|segment| segment.volume_index + 1)
                        .unwrap_or(1);
                    if !missing_volumes.contains(&next_vol) {
                        missing_volumes.push(next_vol);
                    }
                }

                MemberPlannerState {
                    name: crate::path::sanitize_member_path(
                        self.format,
                        entry.file_header.host_os,
                        &entry.file_header.name,
                    ),
                    extractable,
                    missing_volumes,
                }
            })
            .collect()
    }

    /// Get archive metadata without decompression.
    pub fn metadata(&self) -> ArchiveMetadata {
        let members: Vec<MemberInfo> = self
            .members
            .iter()
            .filter(|entry| !entry.file_header.split_before || entry.segments.len() > 1)
            .map(|entry| self.make_member_info(entry))
            .collect();

        ArchiveMetadata {
            format: self.format,
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
            original_name_bytes: self.original_name_raw.clone(),
            original_creation_time: self.original_creation_time,
            volume_count: self.volume_set.expected_count(),
            members,
        }
    }

    /// Return member spans for topology building, including unresolved
    /// continuation entries whose starting header has not arrived yet.
    pub fn topology_members(&self) -> Vec<TopologyMemberInfo> {
        self.members
            .iter()
            .map(|entry| TopologyMemberInfo {
                name: crate::path::sanitize_member_path(
                    self.format,
                    entry.file_header.host_os,
                    &entry.file_header.name,
                ),
                name_raw: entry.file_header.name_raw.clone(),
                unpacked_size: entry.file_header.unpacked_size,
                is_directory: entry.file_header.is_directory,
                volumes: VolumeSpan {
                    first_volume: entry.segments.first().map(|s| s.volume_index).unwrap_or(0),
                    last_volume: entry.segments.last().map(|s| s.volume_index).unwrap_or(0),
                },
                missing_start: entry.file_header.split_before && entry.segments.len() == 1,
            })
            .collect()
    }

    /// Return the set of volumes already integrated into this archive view.
    pub fn present_volumes(&self) -> Vec<usize> {
        self.volume_set
            .presence()
            .into_iter()
            .enumerate()
            .filter_map(|(idx, present)| present.then_some(idx))
            .collect()
    }

    /// Return whether a specific volume index is already integrated.
    pub fn has_volume(&self, volume: usize) -> bool {
        self.volume_set.is_present(volume)
    }

    /// Attach a reader for a volume already known to the archive topology.
    ///
    /// This is used when restoring a cached header snapshot after some early
    /// source volumes were eagerly deleted. Topology still comes from the
    /// cached headers; the attached reader just makes the remaining on-disk
    /// volume available for extraction and readiness checks.
    pub fn attach_volume_reader(&mut self, volume: usize, reader: Box<dyn ReadSeek>) {
        self.volume_set.add_volume(volume);
        self.store_volume_reader(volume, reader);
    }

    /// List all member names.
    pub fn member_names(&self) -> Vec<&str> {
        self.members
            .iter()
            .map(|m| m.file_header.name.as_str())
            .collect()
    }

    /// Find a member by name, returning its index.
    pub fn find_member(&self, name: &str) -> Option<usize> {
        self.members.iter().position(|m| m.file_header.name == name)
    }

    /// Find a member by sanitized name, returning its index into `self.members`.
    ///
    /// Unlike `find_member` (which matches raw header names), this sanitizes
    /// each member's name before comparison. Use this when the caller has a
    /// sanitized name (e.g., from the pipeline topology).
    pub fn find_member_sanitized(&self, sanitized_name: &str) -> Option<usize> {
        self.members.iter().position(|m| {
            crate::path::sanitize_member_path(
                self.format,
                m.file_header.host_os,
                &m.file_header.name,
            ) == sanitized_name
        })
    }

    /// Get member info by index.
    pub fn member_info(&self, index: usize) -> Option<MemberInfo> {
        self.members
            .get(index)
            .map(|entry| self.make_member_info(entry))
    }

    pub(super) fn make_member_info(&self, entry: &MemberEntry) -> MemberInfo {
        let fh = &entry.file_header;
        let first_vol = entry.segments.first().map(|s| s.volume_index).unwrap_or(0);
        let last_vol = entry
            .segments
            .last()
            .map(|s| s.volume_index)
            .unwrap_or(first_vol);
        let total_compressed: u64 = entry.segments.iter().map(|s| s.data_size).sum();

        let rar4_unix_symlink = self.format == ArchiveFormat::Rar4
            && matches!(fh.host_os, crate::types::HostOs::Unix)
            && (fh.attributes.0 & 0xF000) == 0xA000;
        let member_name = crate::path::sanitize_member_path(self.format, fh.host_os, &fh.name);
        let (is_symlink, is_hardlink, is_file_copy, link_target, link_target_bytes) =
            match &entry.redirection {
            Some(redir) => {
                let is_sym = matches!(
                    redir.redir_type,
                    header::RedirectionType::UnixSymlink
                        | header::RedirectionType::WindowsSymlink
                        | header::RedirectionType::WindowsJunction
                );
                let is_hard = matches!(redir.redir_type, header::RedirectionType::Hardlink);
                let is_copy = matches!(redir.redir_type, header::RedirectionType::FileCopy);
                (
                    is_sym,
                    is_hard,
                    is_copy,
                    Some(redir.target.clone()),
                    redir.target_raw.clone(),
                )
            }
            None => (rar4_unix_symlink, false, false, None, None),
        };
        MemberInfo {
            name: member_name,
            raw_name: fh.name.clone(),
            raw_name_bytes: fh.name_raw.clone(),
            unpacked_size: fh.unpacked_size,
            compressed_size: total_compressed,
            is_directory: fh.is_directory,
            crc32: fh.data_crc32,
            mtime: fh.mtime,
            ctime: fh.ctime,
            atime: fh.atime,
            version: fh.version,
            host_os: fh.host_os,
            compression: fh.compression,
            is_encrypted: entry.is_encrypted,
            hash: entry.hash.clone(),
            attributes: fh.attributes,
            owner: entry.owner.clone(),
            volumes: VolumeSpan {
                first_volume: first_vol,
                last_volume: last_vol,
            },
            is_symlink,
            is_hardlink,
            is_file_copy,
            link_target,
            link_target_bytes,
        }
    }

    /// Returns the archive format.
    pub fn format(&self) -> ArchiveFormat {
        self.format
    }

    /// Returns whether the archive is solid.
    pub fn is_solid(&self) -> bool {
        self.is_solid
    }

    /// Returns whether the archive includes a recovery record.
    pub fn has_recovery_record(&self) -> bool {
        self.has_recovery_record
    }

    /// Returns whether the archive is locked.
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }

    /// Returns whether the archive includes authenticity verification/auth info.
    pub fn has_authenticity_verification(&self) -> bool {
        self.has_authenticity_verification
    }

    /// Returns whether more volumes follow.
    pub fn more_volumes(&self) -> bool {
        self.more_volumes
    }

    /// Returns a reference to the volume set.
    pub fn volume_set(&self) -> &VolumeSet {
        &self.volume_set
    }

    /// Reset solid decoder state (e.g. if starting a fresh extraction pass).
    pub fn reset_solid_state(&mut self) {
        self.solid_decoder = None;
        self.solid_decoder_rar4 = None;
        self.solid_next_index = 0;
    }

    /// Get the data segments for a member by index.
    ///
    /// Used by streaming extraction to know which volumes/offsets to read.
    pub fn member_segments(&self, index: usize) -> Option<&[DataSegment]> {
        self.members.get(index).map(|e| e.segments.as_slice())
    }

    /// Get the compression method for a member by index.
    pub fn member_compression(&self, index: usize) -> Option<CompressionMethod> {
        self.members
            .get(index)
            .map(|e| e.file_header.compression.method)
    }

    /// Whether a member is encrypted.
    pub fn member_is_encrypted(&self, index: usize) -> bool {
        self.members.get(index).is_some_and(|e| e.is_encrypted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FileAttributes, HostOs};
    use std::time::{Duration, UNIX_EPOCH};

    fn test_file_header(name: &str) -> FileHeader {
        FileHeader {
            name: name.to_string(),
            name_raw: Some(name.as_bytes().to_vec()),
            unpacked_size: Some(0),
            attributes: FileAttributes(0o644),
            mtime: None,
            ctime: None,
            atime: None,
            data_crc32: None,
            data_hash: None,
            compression: crate::types::CompressionInfo {
                format: ArchiveFormat::Rar5,
                version: 0,
                solid: false,
                method: CompressionMethod::Store,
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
        }
    }

    fn encode_main_extra_record(record_type: u64, body: &[u8]) -> Vec<u8> {
        let type_bytes = crate::vint::encode_vint(record_type);
        let mut result = Vec::new();
        result.extend_from_slice(&crate::vint::encode_vint(
            (type_bytes.len() + body.len()) as u64,
        ));
        result.extend_from_slice(&type_bytes);
        result.extend_from_slice(body);
        result
    }

    fn build_rar5_single_header_archive(
        header_type: u64,
        header_flags: u64,
        type_body: &[u8],
    ) -> Vec<u8> {
        let mut archive = crate::signature::RAR5_SIGNATURE.to_vec();
        archive.extend_from_slice(&build_rar5_raw_header(header_type, header_flags, type_body));
        archive
    }

    fn build_rar5_raw_header(header_type: u64, header_flags: u64, type_body: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&crate::vint::encode_vint(header_type));
        body.extend_from_slice(&crate::vint::encode_vint(header_flags));
        body.extend_from_slice(type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = crate::vint::encode_vint(header_size);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_size_bytes);
        hasher.update(&body);

        let mut raw = Vec::new();
        raw.extend_from_slice(&hasher.finalize().to_le_bytes());
        raw.extend_from_slice(&header_size_bytes);
        raw.extend_from_slice(&body);
        raw
    }

    fn build_rar5_main_header_archive(extra_area: &[u8]) -> Vec<u8> {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&crate::vint::encode_vint(extra_area.len() as u64));
        type_body.extend_from_slice(&crate::vint::encode_vint(
            header::main_archive::flags::RECOVERY_RECORD,
        ));
        type_body.extend_from_slice(extra_area);
        build_rar5_single_header_archive(1, header::common::flags::EXTRA_AREA, &type_body)
    }

    fn archive_with_redirection(member_name: &str, target: &str) -> RarArchive {
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
            volume_set: VolumeSet::new(),
            members: vec![MemberEntry {
                file_header: test_file_header(member_name),
                is_encrypted: false,
                file_encryption: None,
                rar4_salt: None,
                hash: None,
                redirection: Some(header::Redirection {
                    redir_type: header::RedirectionType::UnixSymlink,
                    target: target.to_string(),
                    target_raw: Some(target.as_bytes().to_vec()),
                    target_is_directory: false,
                }),
                owner: None,
                segments: Vec::new(),
            }],
            services: Vec::new(),
            more_volumes: false,
            volumes: Vec::new(),
            solid_decoder: None,
            solid_decoder_rar4: None,
            solid_next_index: 0,
            limits: Limits::default(),
            password: None,
            kdf_cache: Arc::new(crate::crypto::KdfCache::new()),
        }
    }

    fn archive_with_member(
        format: ArchiveFormat,
        host_os: HostOs,
        member_name: &str,
    ) -> RarArchive {
        let mut archive = archive_with_redirection(member_name, "");
        archive.format = format;
        archive.members[0].redirection = None;
        archive.members[0].file_header.host_os = host_os;
        archive.members[0].file_header.compression.format = format;
        archive
    }

    #[test]
    fn metadata_exposes_rar5_main_header_locator_and_metadata_extras() {
        let creation_nanos = 1_700_000_123_456_789_000u64;

        let mut locator = Vec::new();
        locator.extend_from_slice(&crate::vint::encode_vint(0x03)); // QLIST | RR.
        locator.extend_from_slice(&crate::vint::encode_vint(512));
        locator.extend_from_slice(&crate::vint::encode_vint(1024));

        let mut metadata = Vec::new();
        metadata.extend_from_slice(&crate::vint::encode_vint(0x0f)); // NAME | CTIME | UNIXTIME | UNIX_NS.
        metadata.extend_from_slice(&crate::vint::encode_vint(12));
        metadata.extend_from_slice(b"release.rar\0");
        metadata.extend_from_slice(&creation_nanos.to_le_bytes());

        let mut extra_area = Vec::new();
        extra_area.extend_from_slice(&encode_main_extra_record(0x01, &locator));
        extra_area.extend_from_slice(&encode_main_extra_record(0x02, &metadata));

        let archive_bytes = build_rar5_main_header_archive(&extra_area);
        let archive = RarArchive::open(std::io::Cursor::new(archive_bytes)).unwrap();
        let metadata = archive.metadata();

        assert!(metadata.has_recovery_record);
        assert!(metadata.has_locator);
        assert_eq!(metadata.quick_open_offset, Some(8 + 512));
        assert_eq!(metadata.recovery_record_offset, Some(8 + 1024));
        assert_eq!(metadata.original_name.as_deref(), Some("release.rar"));
        assert_eq!(
            metadata.original_name_bytes.as_deref(),
            Some(&b"release.rar\0"[..])
        );
        assert_eq!(
            metadata.original_creation_time,
            Some(UNIX_EPOCH + Duration::new(1_700_000_123, 456_789_000))
        );
    }

    #[test]
    fn metadata_hydrates_rar5_rr_service_from_locator_like_unrar() {
        let rr_offset = 256usize;
        let mut locator = Vec::new();
        locator.extend_from_slice(&crate::vint::encode_vint(0x02)); // RR only.
        locator.extend_from_slice(&crate::vint::encode_vint((rr_offset - 8) as u64));
        let extra_area = encode_main_extra_record(0x01, &locator);

        let mut main_body = Vec::new();
        main_body.extend_from_slice(&crate::vint::encode_vint(extra_area.len() as u64));
        main_body.extend_from_slice(&crate::vint::encode_vint(
            header::main_archive::flags::RECOVERY_RECORD,
        ));
        main_body.extend_from_slice(&extra_area);

        let mut end_body = Vec::new();
        end_body.extend_from_slice(&crate::vint::encode_vint(0));

        let mut archive_bytes = crate::signature::RAR5_SIGNATURE.to_vec();
        archive_bytes.extend_from_slice(&build_rar5_raw_header(
            1,
            header::common::flags::EXTRA_AREA,
            &main_body,
        ));
        archive_bytes.extend_from_slice(&build_rar5_raw_header(5, 0, &end_body));
        assert!(archive_bytes.len() < rr_offset);
        archive_bytes.resize(rr_offset, 0);

        let percent = crate::vint::encode_vint(42);
        let rr_extra = encode_main_extra_record(0x07, &percent);
        let mut rr_body = Vec::new();
        rr_body.extend_from_slice(&crate::vint::encode_vint(rr_extra.len() as u64));
        rr_body.extend_from_slice(&crate::vint::encode_vint(0)); // file flags
        rr_body.extend_from_slice(&crate::vint::encode_vint(0)); // unpacked size
        rr_body.extend_from_slice(&crate::vint::encode_vint(0)); // attributes
        rr_body.extend_from_slice(&crate::vint::encode_vint(0)); // compression info
        rr_body.extend_from_slice(&crate::vint::encode_vint(0)); // host OS
        rr_body.extend_from_slice(&crate::vint::encode_vint(2)); // name length
        rr_body.extend_from_slice(b"RR");
        rr_body.extend_from_slice(&rr_extra);
        archive_bytes.extend_from_slice(&build_rar5_raw_header(
            3,
            header::common::flags::EXTRA_AREA,
            &rr_body,
        ));

        let archive = RarArchive::open(std::io::Cursor::new(archive_bytes)).unwrap();
        let metadata = archive.metadata();

        assert_eq!(metadata.recovery_record_offset, Some(rr_offset as u64));
        assert_eq!(metadata.recovery_records.len(), 1);
        let record = &metadata.recovery_records[0];
        assert_eq!(record.kind, crate::types::RecoveryRecordKind::Rar5Service);
        assert_eq!(record.offset, rr_offset as u64);
        assert_eq!(record.recovery_percent, Some(42));
    }

    #[test]
    fn open_rejects_future_rar5_archive_encryption_version() {
        let archive_bytes = build_rar5_single_header_archive(4, 0, &crate::vint::encode_vint(1));

        match RarArchive::open(std::io::Cursor::new(archive_bytes)) {
            Err(RarError::UnsupportedEncryption { version: 1 }) => {}
            Err(err) => panic!("expected unsupported encryption version, got {err}"),
            Ok(_) => panic!("expected unsupported encryption version, got success"),
        }
    }

    #[test]
    fn member_info_exposes_safe_link_target() {
        let archive = archive_with_redirection("dir/link", "../target.txt");
        let info = archive.member_info(0).unwrap();

        assert!(info.is_symlink);
        assert_eq!(info.link_target.as_deref(), Some("../target.txt"));
        assert_eq!(
            info.link_target_bytes.as_deref(),
            Some(&b"../target.txt"[..])
        );
    }

    #[test]
    fn member_info_preserves_unsafe_link_target_for_extraction_error() {
        let archive = archive_with_redirection("dir/link", "/etc/passwd");
        let info = archive.member_info(0).unwrap();

        assert!(info.is_symlink);
        assert!(!info.is_hardlink);
        assert_eq!(info.link_target.as_deref(), Some("/etc/passwd"));
    }

    #[test]
    fn member_info_uses_unrar_rar5_windows_backslash_conversion() {
        let archive = archive_with_member(ArchiveFormat::Rar5, HostOs::Windows, "dir\\file.txt");
        let info = archive.member_info(0).unwrap();

        assert_eq!(info.name, "dir_file.txt");
        assert_eq!(info.raw_name_bytes.as_deref(), Some(&b"dir\\file.txt"[..]));
        assert_eq!(archive.find_member_sanitized("dir_file.txt"), Some(0));
        assert_eq!(archive.topology_members()[0].name, "dir_file.txt");
        assert_eq!(
            archive.topology_members()[0].name_raw.as_deref(),
            Some(&b"dir\\file.txt"[..])
        );
    }

    #[test]
    fn member_info_uses_unrar_rar5_unix_backslash_conversion() {
        let archive = archive_with_member(ArchiveFormat::Rar5, HostOs::Unix, "dir\\file.txt");
        let info = archive.member_info(0).unwrap();

        assert_eq!(info.name, "dir\\file.txt");
        assert_eq!(info.raw_name_bytes.as_deref(), Some(&b"dir\\file.txt"[..]));
        assert_eq!(archive.find_member_sanitized("dir\\file.txt"), Some(0));
        assert_eq!(archive.find_member_sanitized("dir/file.txt"), None);
    }

    #[test]
    fn member_info_uses_unrar_rar4_backslash_separator_conversion() {
        let archive = archive_with_member(ArchiveFormat::Rar4, HostOs::Windows, "dir\\file.txt");
        let info = archive.member_info(0).unwrap();

        assert_eq!(info.name, "dir/file.txt");
        assert_eq!(info.raw_name_bytes.as_deref(), Some(&b"dir\\file.txt"[..]));
        assert_eq!(archive.find_member_sanitized("dir/file.txt"), Some(0));
    }
}
