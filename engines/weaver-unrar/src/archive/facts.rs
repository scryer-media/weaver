use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use super::{PackedDataHash, RarArchive, ReadSeek};
use crate::error::RarResult;
use crate::signature;
use crate::types::{ArchiveFormat, CompressionMethod};

/// Host OS families Scryer needs to reason about.
///
/// RAR4 has additional historical host identifiers such as MS-DOS, OS/2 and
/// BeOS. Scryer only needs Darwin/macOS, Linux/Unix and Windows behavior, so
/// volume facts leave the legacy targets unmapped instead of pretending they
/// are supported modern archive origins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RarVolumeHostOs {
    Windows,
    Unix,
    Darwin,
}

/// Unix owner/group metadata from RAR5 `FHEXTRA_UOWNER`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeUnixOwnerFacts {
    #[serde(default)]
    pub user_name: Option<String>,
    #[serde(default)]
    pub group_name: Option<String>,
    #[serde(default)]
    pub user_name_raw: Option<Vec<u8>>,
    #[serde(default)]
    pub group_name_raw: Option<Vec<u8>>,
    #[serde(default)]
    pub uid: Option<u64>,
    #[serde(default)]
    pub gid: Option<u64>,
}

/// Immutable header facts parsed from a single physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeFacts {
    pub format: u8,
    pub volume_number: u32,
    pub more_volumes: bool,
    pub is_solid: bool,
    pub is_encrypted: bool,
    #[serde(default)]
    pub is_volume: bool,
    #[serde(default)]
    pub has_recovery_record: bool,
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
    pub members: Vec<RarVolumeMemberFacts>,
    #[serde(default)]
    pub services: Vec<RarVolumeServiceFacts>,
}

/// A single ordered file-header record from one physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeMemberFacts {
    pub order: u32,
    pub name: String,
    #[serde(default)]
    pub name_raw: Option<Vec<u8>>,
    pub unpacked_size: Option<u64>,
    pub data_crc32: Option<u32>,
    #[serde(default)]
    pub data_blake2_hash: Option<[u8; 32]>,
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub packed_crc32: Option<u32>,
    #[serde(default)]
    pub packed_blake2_hash: Option<[u8; 32]>,
    #[serde(default)]
    pub packed_hash_uses_mac: bool,
    pub split_before: bool,
    pub split_after: bool,
    pub is_directory: bool,
    pub is_encrypted: bool,
    #[serde(default)]
    pub host_os: Option<RarVolumeHostOs>,
    #[serde(default)]
    pub attributes: Option<u64>,
    #[serde(default)]
    pub owner: Option<RarVolumeUnixOwnerFacts>,
    #[serde(default)]
    pub mtime_ns: Option<u64>,
    #[serde(default)]
    pub ctime_ns: Option<u64>,
    #[serde(default)]
    pub atime_ns: Option<u64>,
    pub data_offset: u64,
    pub data_size: u64,
    pub compression_method: u8,
    pub compression_version: u8,
    pub compression_solid: bool,
    pub dict_size: u64,
    pub use_hash_mac: bool,
    pub redirection_type: Option<u64>,
    pub redirection_target: Option<String>,
    #[serde(default)]
    pub redirection_target_raw: Option<Vec<u8>>,
    pub redirection_target_is_directory: bool,
}

/// A service-header record from one physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeServiceFacts {
    pub order: u32,
    pub name: String,
    #[serde(default)]
    pub name_raw: Option<Vec<u8>>,
    #[serde(default)]
    pub subtype: Option<u16>,
    #[serde(default)]
    pub level: Option<u8>,
    #[serde(default)]
    pub is_child: bool,
    #[serde(default)]
    pub is_inherited: bool,
    pub unpacked_size: Option<u64>,
    pub data_crc32: Option<u32>,
    #[serde(default)]
    pub data_blake2_hash: Option<[u8; 32]>,
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub comment_crc16: Option<u16>,
    #[serde(default)]
    pub packed_crc32: Option<u32>,
    #[serde(default)]
    pub packed_blake2_hash: Option<[u8; 32]>,
    #[serde(default)]
    pub packed_hash_uses_mac: bool,
    pub split_before: bool,
    pub split_after: bool,
    pub is_encrypted: bool,
    #[serde(default)]
    pub host_os: Option<RarVolumeHostOs>,
    #[serde(default)]
    pub attributes: Option<u64>,
    #[serde(default)]
    pub mtime_ns: Option<u64>,
    #[serde(default)]
    pub ctime_ns: Option<u64>,
    #[serde(default)]
    pub atime_ns: Option<u64>,
    pub data_offset: u64,
    pub data_size: u64,
    pub compression_method: u8,
    pub compression_version: u8,
    pub compression_solid: bool,
    pub dict_size: u64,
    pub use_hash_mac: bool,
    #[serde(default)]
    pub stream_name: Option<String>,
    #[serde(default)]
    pub stream_name_raw: Option<Vec<u8>>,
}

impl RarVolumeFacts {
    pub fn archive_format(&self) -> ArchiveFormat {
        match self.format {
            14 => ArchiveFormat::Rar14,
            4 => ArchiveFormat::Rar4,
            _ => ArchiveFormat::Rar5,
        }
    }
}

fn supported_host_os(host_os: crate::types::HostOs) -> Option<RarVolumeHostOs> {
    match host_os {
        crate::types::HostOs::Windows => Some(RarVolumeHostOs::Windows),
        crate::types::HostOs::Unix => Some(RarVolumeHostOs::Unix),
        crate::types::HostOs::Darwin => Some(RarVolumeHostOs::Darwin),
        crate::types::HostOs::Unknown(_) => None,
    }
}

fn supported_rar4_host_os(host_os: crate::rar4::types::Rar4HostOs) -> Option<RarVolumeHostOs> {
    match host_os {
        crate::rar4::types::Rar4HostOs::Windows => Some(RarVolumeHostOs::Windows),
        crate::rar4::types::Rar4HostOs::Unix => Some(RarVolumeHostOs::Unix),
        // RAR4's Mac host id is the only Mac-family archive marker available.
        // Scryer only distinguishes supported Darwin/macOS, Linux/Unix, and
        // Windows targets, so older MS-DOS/OS2/BeOS host ids stay unmapped.
        crate::rar4::types::Rar4HostOs::MacOs => Some(RarVolumeHostOs::Darwin),
        crate::rar4::types::Rar4HostOs::MsDos
        | crate::rar4::types::Rar4HostOs::Os2
        | crate::rar4::types::Rar4HostOs::BeOs
        | crate::rar4::types::Rar4HostOs::Unknown(_) => None,
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

fn file_hash_blake2(hash: Option<&crate::types::FileHash>) -> Option<[u8; 32]> {
    hash.map(|hash| match hash {
        crate::types::FileHash::Blake2sp(value) => *value,
    })
}

fn unix_owner_facts(owner: Option<crate::types::UnixOwnerInfo>) -> Option<RarVolumeUnixOwnerFacts> {
    owner.map(|owner| RarVolumeUnixOwnerFacts {
        user_name: owner.user_name,
        group_name: owner.group_name,
        user_name_raw: owner.user_name_raw,
        group_name_raw: owner.group_name_raw,
        uid: owner.uid,
        gid: owner.gid,
    })
}

impl RarVolumeServiceFacts {
    fn from_rar4_service(order: usize, fh: &crate::rar4::types::Rar4FileHeader) -> Self {
        let file_header = RarArchive::rar4_to_file_header(fh, false);
        let (packed_crc32, packed_blake2_hash) = split_packed_hash(
            RarArchive::packed_hash_for_split_segment(&file_header, None),
        );
        Self {
            order: order as u32,
            name: fh.name.clone(),
            name_raw: fh.name_raw.clone(),
            subtype: None,
            level: None,
            is_child: false,
            is_inherited: false,
            unpacked_size: fh.unpacked_size,
            data_crc32: Some(fh.crc32),
            data_blake2_hash: None,
            version: file_header.version,
            comment_crc16: None,
            packed_crc32,
            packed_blake2_hash,
            packed_hash_uses_mac: false,
            split_before: fh.split_before,
            split_after: fh.split_after,
            is_encrypted: fh.is_encrypted,
            host_os: supported_rar4_host_os(fh.host_os),
            attributes: Some(u64::from(fh.attributes)),
            mtime_ns: encode_system_time(file_header.mtime),
            ctime_ns: encode_system_time(file_header.ctime),
            atime_ns: encode_system_time(file_header.atime),
            data_offset: fh.data_offset,
            data_size: fh.packed_size,
            compression_method: file_header.compression.method.code(),
            compression_version: fh.unpack_version,
            compression_solid: file_header.compression.solid,
            dict_size: fh.dict_size,
            use_hash_mac: false,
            stream_name: None,
            stream_name_raw: None,
        }
    }

    fn from_rar4_comment(order: usize, comment: &crate::rar4::types::Rar4CommentHeader) -> Self {
        Self {
            order: order as u32,
            name: "CMT".to_string(),
            name_raw: Some(b"CMT".to_vec()),
            subtype: None,
            level: None,
            is_child: false,
            is_inherited: false,
            unpacked_size: Some(u64::from(comment.unpacked_size)),
            data_crc32: None,
            data_blake2_hash: None,
            version: None,
            comment_crc16: Some(comment.crc16),
            packed_crc32: None,
            packed_blake2_hash: None,
            packed_hash_uses_mac: false,
            split_before: false,
            split_after: false,
            is_encrypted: false,
            host_os: None,
            attributes: None,
            mtime_ns: None,
            ctime_ns: None,
            atime_ns: None,
            data_offset: comment.data_offset,
            data_size: comment.packed_size,
            compression_method: rar4_method_to_compression_method(comment.method).code(),
            compression_version: comment.unpack_version,
            compression_solid: false,
            dict_size: 0,
            use_hash_mac: false,
            stream_name: None,
            stream_name_raw: None,
        }
    }

    fn from_rar4_old_service(
        order: usize,
        service: &crate::rar4::types::Rar4OldServiceHeader,
    ) -> Self {
        use crate::rar4::types::Rar4OldServiceData;

        let (
            name,
            unpacked_size,
            data_crc32,
            compression_method,
            compression_version,
            stream_name,
            stream_name_raw,
        ) = match &service.data {
            Rar4OldServiceData::UnixOwner => (
                "UOW".to_string(),
                Some(service.data_size),
                None,
                CompressionMethod::Store.code(),
                0,
                None,
                None,
            ),
            Rar4OldServiceData::NtAcl {
                unpacked_size,
                unpack_version,
                method,
                crc32,
            } => (
                "ACL".to_string(),
                Some(u64::from(*unpacked_size)),
                Some(*crc32),
                rar4_method_to_compression_method(*method).code(),
                *unpack_version,
                None,
                None,
            ),
            Rar4OldServiceData::Stream {
                unpacked_size,
                unpack_version,
                method,
                crc32,
                stream_name,
                stream_name_raw,
            } => (
                "STM".to_string(),
                Some(u64::from(*unpacked_size)),
                Some(*crc32),
                rar4_method_to_compression_method(*method).code(),
                *unpack_version,
                Some(stream_name.clone()),
                Some(stream_name_raw.clone()),
            ),
            Rar4OldServiceData::Unknown => (
                format!("0x{:04x}", service.subtype),
                None,
                None,
                CompressionMethod::Store.code(),
                0,
                None,
                None,
            ),
        };

        Self {
            order: order as u32,
            name,
            name_raw: None,
            subtype: Some(service.subtype),
            level: Some(service.level),
            is_child: false,
            is_inherited: false,
            unpacked_size,
            data_crc32,
            data_blake2_hash: None,
            version: None,
            comment_crc16: None,
            packed_crc32: None,
            packed_blake2_hash: None,
            packed_hash_uses_mac: false,
            split_before: false,
            split_after: false,
            is_encrypted: false,
            host_os: None,
            attributes: None,
            mtime_ns: None,
            ctime_ns: None,
            atime_ns: None,
            data_offset: service.data_offset,
            data_size: service.data_size,
            compression_method,
            compression_version,
            compression_solid: false,
            dict_size: 0x10000,
            use_hash_mac: false,
            stream_name,
            stream_name_raw,
        }
    }

    fn from_rar5_service(order: usize, service: crate::header::ParsedService) -> Self {
        let packed_hash =
            RarArchive::packed_hash_for_split_segment(&service.header.inner, service.hash.as_ref());
        let (packed_crc32, packed_blake2_hash) = split_packed_hash(packed_hash);
        let packed_hash_uses_mac = packed_hash.is_some()
            && service
                .file_encryption
                .as_ref()
                .is_some_and(|info| info.use_hash_mac);
        Self {
            order: order as u32,
            name: service.header.service_name().to_string(),
            name_raw: service.header.inner.name_raw.clone(),
            subtype: None,
            level: None,
            is_child: service.header.is_child,
            is_inherited: service.header.is_inherited,
            unpacked_size: service.header.inner.unpacked_size,
            data_crc32: service.header.inner.data_crc32,
            data_blake2_hash: file_hash_blake2(service.hash.as_ref()),
            version: service.header.inner.version,
            comment_crc16: None,
            packed_crc32,
            packed_blake2_hash,
            packed_hash_uses_mac,
            split_before: service.header.inner.split_before,
            split_after: service.header.inner.split_after,
            is_encrypted: service.is_encrypted,
            host_os: supported_host_os(service.header.inner.host_os),
            attributes: Some(service.header.inner.attributes.0),
            mtime_ns: encode_system_time(service.header.inner.mtime),
            ctime_ns: encode_system_time(service.header.inner.ctime),
            atime_ns: encode_system_time(service.header.inner.atime),
            data_offset: service.header.inner.data_offset,
            data_size: service.header.inner.data_size,
            compression_method: service.header.inner.compression.method.code(),
            compression_version: service.header.inner.compression.version,
            compression_solid: service.header.inner.compression.solid,
            dict_size: service.header.inner.compression.dict_size,
            use_hash_mac: service
                .file_encryption
                .as_ref()
                .is_some_and(|info| info.use_hash_mac),
            stream_name: None,
            stream_name_raw: None,
        }
    }
}

fn split_packed_hash(packed_hash: Option<PackedDataHash>) -> (Option<u32>, Option<[u8; 32]>) {
    match packed_hash {
        Some(PackedDataHash::Crc32(value)) => (Some(value), None),
        Some(PackedDataHash::Blake2sp(value)) => (None, Some(value)),
        None => (None, None),
    }
}

fn rar4_method_to_compression_method(method: crate::rar4::types::Rar4Method) -> CompressionMethod {
    match method {
        crate::rar4::types::Rar4Method::Store => CompressionMethod::Store,
        crate::rar4::types::Rar4Method::Fastest => CompressionMethod::Fastest,
        crate::rar4::types::Rar4Method::Fast => CompressionMethod::Fast,
        crate::rar4::types::Rar4Method::Normal => CompressionMethod::Normal,
        crate::rar4::types::Rar4Method::Good => CompressionMethod::Good,
        crate::rar4::types::Rar4Method::Best => CompressionMethod::Best,
        crate::rar4::types::Rar4Method::Unknown(code) => CompressionMethod::Unknown(code),
    }
}

impl RarArchive {
    /// Parse immutable facts from a single physical RAR volume without
    /// building a live multi-volume archive graph.
    pub fn parse_volume_facts(
        reader: impl std::io::Read + std::io::Seek + Send + 'static,
        password: Option<&str>,
    ) -> RarResult<RarVolumeFacts> {
        let reader: Box<dyn ReadSeek> = Box::new(reader);
        Self::parse_volume_facts_boxed(reader, password)
    }

    pub(crate) fn parse_volume_facts_boxed(
        mut reader: Box<dyn ReadSeek>,
        password: Option<&str>,
    ) -> RarResult<RarVolumeFacts> {
        use std::io::{Seek, SeekFrom};

        reader
            .seek(SeekFrom::Start(0))
            .map_err(crate::error::RarError::Io)?;
        let format = signature::read_signature(&mut reader)?;

        if format.is_rar4_family() {
            let parsed = if format == ArchiveFormat::Rar14 {
                crate::rar4::parse_rar14_headers(&mut reader)?
            } else {
                crate::rar4::parse_rar4_headers(&mut reader, password)?
            };
            let volume_number = parsed
                .end
                .as_ref()
                .and_then(|end| end.volume_number)
                .map(|value| value as u32)
                .unwrap_or(0);
            let more_volumes = parsed.end.as_ref().is_some_and(|end| end.more_volumes)
                || (format == ArchiveFormat::Rar14 && parsed.files.iter().any(|f| f.split_after));
            let has_rar4_uowner_payload =
                parsed.services.iter().any(|service| service.name == "UOW")
                    || parsed.old_services.iter().any(|service| {
                        matches!(
                            &service.data,
                            crate::rar4::types::Rar4OldServiceData::UnixOwner
                        )
                    });
            let member_owner_facts: Vec<_> = if has_rar4_uowner_payload {
                let archive = Self::open_rar4_parsed(
                    reader,
                    password,
                    std::sync::Arc::new(crate::crypto::KdfCache::new()),
                    format,
                    parsed.clone(),
                )?;
                archive
                    .members
                    .iter()
                    .map(|member| unix_owner_facts(member.owner.clone()))
                    .collect()
            } else {
                parsed
                    .files
                    .iter()
                    .map(|fh| unix_owner_facts(fh.owner.clone()))
                    .collect()
            };
            let members = parsed
                .files
                .iter()
                .enumerate()
                .map(|(order, fh)| {
                    let file_header =
                        RarArchive::rar4_to_file_header(fh, parsed.archive_header.is_solid);
                    let (packed_crc32, packed_blake2_hash) = split_packed_hash(
                        RarArchive::packed_hash_for_split_segment(&file_header, None),
                    );
                    RarVolumeMemberFacts {
                        order: order as u32,
                        name: fh.name.clone(),
                        name_raw: fh.name_raw.clone(),
                        unpacked_size: fh.unpacked_size,
                        data_crc32: (!fh.is_rar14).then_some(fh.crc32),
                        data_blake2_hash: None,
                        version: file_header.version,
                        packed_crc32,
                        packed_blake2_hash,
                        packed_hash_uses_mac: false,
                        split_before: fh.split_before,
                        split_after: fh.split_after,
                        is_directory: fh.is_directory,
                        is_encrypted: fh.is_encrypted,
                        host_os: supported_rar4_host_os(fh.host_os),
                        attributes: Some(u64::from(fh.attributes)),
                        owner: member_owner_facts.get(order).cloned().flatten(),
                        mtime_ns: encode_system_time(file_header.mtime),
                        ctime_ns: encode_system_time(file_header.ctime),
                        atime_ns: encode_system_time(file_header.atime),
                        data_offset: fh.data_offset,
                        data_size: fh.packed_size,
                        compression_method: file_header.compression.method.code(),
                        compression_version: fh.unpack_version,
                        compression_solid: RarArchive::rar4_effective_solid(
                            fh,
                            parsed.archive_header.is_solid,
                        ),
                        dict_size: fh.dict_size,
                        use_hash_mac: false,
                        redirection_type: None,
                        redirection_target: None,
                        redirection_target_raw: None,
                        redirection_target_is_directory: false,
                    }
                })
                .collect();
            let mut services: Vec<_> = parsed
                .services
                .iter()
                .enumerate()
                .map(|(order, service)| RarVolumeServiceFacts::from_rar4_service(order, service))
                .collect();
            let base_order = services.len();
            services.extend(parsed.comments.iter().enumerate().map(|(order, comment)| {
                RarVolumeServiceFacts::from_rar4_comment(base_order + order, comment)
            }));
            let base_order = services.len();
            services.extend(
                parsed
                    .old_services
                    .iter()
                    .enumerate()
                    .map(|(order, service)| {
                        RarVolumeServiceFacts::from_rar4_old_service(base_order + order, service)
                    }),
            );
            return Ok(RarVolumeFacts {
                format: if format == ArchiveFormat::Rar14 {
                    14
                } else {
                    4
                },
                volume_number,
                more_volumes,
                is_solid: parsed.archive_header.is_solid,
                is_encrypted: parsed.archive_header.is_encrypted,
                is_volume: parsed.archive_header.is_volume,
                has_recovery_record: parsed.archive_header.has_recovery_record,
                is_locked: parsed.archive_header.is_locked,
                has_authenticity_verification: parsed.archive_header.has_authenticity_verification,
                has_locator: false,
                quick_open_offset: None,
                recovery_record_offset: None,
                original_name: None,
                original_name_raw: None,
                original_creation_time_ns: None,
                members,
                services,
            });
        }

        let parsed = crate::header::parse_all_headers(&mut reader, password)?;
        let main = parsed.main.as_ref();
        let volume_number = main.and_then(|main| main.volume_number).unwrap_or(0) as u32;
        let more_volumes = parsed.end.as_ref().is_some_and(|end| end.more_volumes);
        let is_solid = main.is_some_and(|main| main.is_solid);
        let is_volume = main.is_some_and(|main| main.is_volume);
        let has_recovery_record = main.is_some_and(|main| main.has_recovery_record);
        let is_locked = main.is_some_and(|main| main.is_locked);
        let has_locator = main.is_some_and(|main| main.has_locator);
        let quick_open_offset = main.and_then(|main| main.quick_open_offset);
        let recovery_record_offset = main.and_then(|main| main.recovery_record_offset);
        let original_name = main.and_then(|main| main.original_name.clone());
        let original_name_raw = main.and_then(|main| main.original_name_raw.clone());
        let original_creation_time_ns =
            encode_system_time(main.and_then(|main| main.original_creation_time));
        let services = parsed
            .services
            .into_iter()
            .enumerate()
            .map(|(order, service)| RarVolumeServiceFacts::from_rar5_service(order, service))
            .collect();
        let members = parsed
            .files
            .into_iter()
            .enumerate()
            .map(|(order, parsed_file)| {
                let packed_hash = RarArchive::packed_hash_for_split_segment(
                    &parsed_file.header,
                    parsed_file.hash.as_ref(),
                );
                let (packed_crc32, packed_blake2_hash) = split_packed_hash(packed_hash);
                let packed_hash_uses_mac = packed_hash.is_some()
                    && parsed_file
                        .file_encryption
                        .as_ref()
                        .is_some_and(|info| info.use_hash_mac);
                RarVolumeMemberFacts {
                    order: order as u32,
                    name: parsed_file.header.name,
                    name_raw: parsed_file.header.name_raw,
                    unpacked_size: parsed_file.header.unpacked_size,
                    data_crc32: parsed_file.header.data_crc32,
                    data_blake2_hash: file_hash_blake2(parsed_file.hash.as_ref()),
                    version: parsed_file.header.version,
                    packed_crc32,
                    packed_blake2_hash,
                    packed_hash_uses_mac,
                    split_before: parsed_file.header.split_before,
                    split_after: parsed_file.header.split_after,
                    is_directory: parsed_file.header.is_directory,
                    is_encrypted: parsed_file.is_encrypted,
                    host_os: supported_host_os(parsed_file.header.host_os),
                    attributes: Some(parsed_file.header.attributes.0),
                    owner: unix_owner_facts(parsed_file.owner),
                    mtime_ns: encode_system_time(parsed_file.header.mtime),
                    ctime_ns: encode_system_time(parsed_file.header.ctime),
                    atime_ns: encode_system_time(parsed_file.header.atime),
                    data_offset: parsed_file.header.data_offset,
                    data_size: parsed_file.header.data_size,
                    compression_method: parsed_file.header.compression.method.code(),
                    compression_version: parsed_file.header.compression.version,
                    compression_solid: parsed_file.header.compression.solid,
                    dict_size: parsed_file.header.compression.dict_size,
                    use_hash_mac: parsed_file
                        .file_encryption
                        .as_ref()
                        .is_some_and(|info| info.use_hash_mac),
                    redirection_type: parsed_file.redirection.as_ref().map(|redir| {
                        match redir.redir_type {
                            crate::header::RedirectionType::UnixSymlink => 1,
                            crate::header::RedirectionType::WindowsSymlink => 2,
                            crate::header::RedirectionType::WindowsJunction => 3,
                            crate::header::RedirectionType::Hardlink => 4,
                            crate::header::RedirectionType::FileCopy => 5,
                            crate::header::RedirectionType::Unknown(value) => value,
                        }
                    }),
                    redirection_target: parsed_file
                        .redirection
                        .as_ref()
                        .map(|redir| redir.target.clone()),
                    redirection_target_raw: parsed_file
                        .redirection
                        .as_ref()
                        .and_then(|redir| redir.target_raw.clone()),
                    redirection_target_is_directory: parsed_file
                        .redirection
                        .as_ref()
                        .is_some_and(|redir| redir.target_is_directory),
                }
            })
            .collect();

        Ok(RarVolumeFacts {
            format: 5,
            volume_number,
            more_volumes,
            is_solid,
            is_encrypted: parsed.is_encrypted,
            is_volume,
            has_recovery_record,
            is_locked,
            has_authenticity_verification: false,
            has_locator,
            quick_open_offset,
            recovery_record_offset,
            original_name,
            original_name_raw,
            original_creation_time_ns,
            members,
            services,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[derive(Serialize)]
    struct LegacyVolumeFacts {
        format: u8,
        volume_number: u32,
        more_volumes: bool,
        is_solid: bool,
        is_encrypted: bool,
        members: Vec<LegacyMemberFacts>,
    }

    #[derive(Serialize)]
    struct LegacyMemberFacts {
        order: u32,
        name: String,
        unpacked_size: Option<u64>,
        data_crc32: Option<u32>,
        split_before: bool,
        split_after: bool,
        is_directory: bool,
        is_encrypted: bool,
        data_offset: u64,
        data_size: u64,
        compression_method: u8,
        compression_version: u8,
        compression_solid: bool,
        dict_size: u64,
        use_hash_mac: bool,
        redirection_type: Option<u64>,
        redirection_target: Option<String>,
        redirection_target_is_directory: bool,
    }

    fn build_rar4_header(header_type: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let mut header = Vec::with_capacity(7 + body.len());
        header.extend_from_slice(&[0, 0]);
        header.push(header_type);
        header.extend_from_slice(&flags.to_le_bytes());
        header.extend_from_slice(&((7 + body.len()) as u16).to_le_bytes());
        header.extend_from_slice(body);
        let crc16 = (crc32fast::hash(&header[2..]) & 0xffff) as u16;
        header[0..2].copy_from_slice(&crc16.to_le_bytes());
        header
    }

    fn build_rar4_main_header(flags: u16, high_pos_av: u16, pos_av: u32) -> Vec<u8> {
        let mut body = Vec::with_capacity(6);
        body.extend_from_slice(&high_pos_av.to_le_bytes());
        body.extend_from_slice(&pos_av.to_le_bytes());
        build_rar4_header(0x73, flags, &body)
    }

    fn signed_rar4_archive_bytes() -> Vec<u8> {
        let mut bytes = crate::signature::RAR4_SIGNATURE.to_vec();
        bytes.extend_from_slice(&build_rar4_main_header(0, 0x1234, 0x5678_9abc));
        bytes.extend_from_slice(&build_rar4_header(0x7b, 0, &[]));
        bytes
    }

    #[test]
    fn named_messagepack_legacy_volume_facts_default_new_fields() {
        let legacy = LegacyVolumeFacts {
            format: 5,
            volume_number: 2,
            more_volumes: true,
            is_solid: false,
            is_encrypted: false,
            members: vec![LegacyMemberFacts {
                order: 0,
                name: "legacy.part03.rar".to_string(),
                unpacked_size: Some(123),
                data_crc32: Some(0x1234_5678),
                split_before: true,
                split_after: false,
                is_directory: false,
                is_encrypted: false,
                data_offset: 44,
                data_size: 55,
                compression_method: 0,
                compression_version: 0,
                compression_solid: false,
                dict_size: 0,
                use_hash_mac: false,
                redirection_type: None,
                redirection_target: None,
                redirection_target_is_directory: false,
            }],
        };
        let encoded = rmp_serde::to_vec_named(&legacy).unwrap();

        let decoded: RarVolumeFacts = rmp_serde::from_slice(&encoded).unwrap();

        assert!(decoded.services.is_empty());
        assert!(!decoded.is_volume);
        assert!(!decoded.has_recovery_record);
        assert!(!decoded.is_locked);
        assert!(!decoded.has_authenticity_verification);
        assert!(!decoded.has_locator);
        assert_eq!(decoded.quick_open_offset, None);
        assert_eq!(decoded.recovery_record_offset, None);
        assert_eq!(decoded.original_name, None);
        assert_eq!(decoded.original_name_raw, None);
        assert_eq!(decoded.original_creation_time_ns, None);
        assert_eq!(decoded.members.len(), 1);
        assert_eq!(decoded.members[0].name_raw, None);
        assert_eq!(decoded.members[0].data_blake2_hash, None);
        assert_eq!(decoded.members[0].version, None);
        assert_eq!(decoded.members[0].redirection_target_raw, None);
        assert_eq!(decoded.members[0].host_os, None);
        assert_eq!(decoded.members[0].attributes, None);
        assert_eq!(decoded.members[0].owner, None);
        assert_eq!(decoded.members[0].mtime_ns, None);
        assert_eq!(decoded.members[0].ctime_ns, None);
        assert_eq!(decoded.members[0].atime_ns, None);
        assert_eq!(decoded.members[0].packed_crc32, None);
        assert_eq!(decoded.members[0].packed_blake2_hash, None);
        assert!(!decoded.members[0].packed_hash_uses_mac);
    }

    #[test]
    fn named_messagepack_legacy_service_facts_default_child_flags() {
        #[derive(Serialize)]
        struct LegacyServiceVolumeFacts {
            format: u8,
            volume_number: u32,
            more_volumes: bool,
            is_solid: bool,
            is_encrypted: bool,
            members: Vec<LegacyMemberFacts>,
            services: Vec<LegacyServiceFacts>,
        }

        #[derive(Serialize)]
        struct LegacyServiceFacts {
            order: u32,
            name: String,
            unpacked_size: Option<u64>,
            data_crc32: Option<u32>,
            split_before: bool,
            split_after: bool,
            is_encrypted: bool,
            data_offset: u64,
            data_size: u64,
            compression_method: u8,
            compression_version: u8,
            compression_solid: bool,
            dict_size: u64,
            use_hash_mac: bool,
            stream_name: Option<String>,
            stream_name_raw: Option<Vec<u8>>,
        }

        let legacy = LegacyServiceVolumeFacts {
            format: 5,
            volume_number: 0,
            more_volumes: false,
            is_solid: false,
            is_encrypted: false,
            members: Vec::new(),
            services: vec![LegacyServiceFacts {
                order: 0,
                name: "CMT".to_string(),
                unpacked_size: Some(3),
                data_crc32: None,
                split_before: false,
                split_after: false,
                is_encrypted: false,
                data_offset: 123,
                data_size: 3,
                compression_method: CompressionMethod::Store.code(),
                compression_version: 0,
                compression_solid: false,
                dict_size: 0,
                use_hash_mac: false,
                stream_name: None,
                stream_name_raw: None,
            }],
        };
        let encoded = rmp_serde::to_vec_named(&legacy).unwrap();

        let decoded: RarVolumeFacts = rmp_serde::from_slice(&encoded).unwrap();

        assert_eq!(decoded.services.len(), 1);
        assert!(!decoded.services[0].is_child);
        assert!(!decoded.services[0].is_inherited);
    }

    #[test]
    fn rar4_volume_facts_preserve_signed_main_header_like_unrar() {
        let facts =
            RarArchive::parse_volume_facts(Cursor::new(signed_rar4_archive_bytes()), None).unwrap();

        assert_eq!(facts.format, 4);
        assert!(facts.has_authenticity_verification);
    }
}
