use serde::{Deserialize, Serialize};

use super::{PackedDataHash, RarArchive, ReadSeek};
use crate::error::RarResult;
use crate::signature;
use crate::types::{ArchiveFormat, CompressionMethod};

/// Immutable header facts parsed from a single physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeFacts {
    pub format: u8,
    pub volume_number: u32,
    pub more_volumes: bool,
    pub is_solid: bool,
    pub is_encrypted: bool,
    pub members: Vec<RarVolumeMemberFacts>,
    #[serde(default)]
    pub services: Vec<RarVolumeServiceFacts>,
}

/// A single ordered file-header record from one physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeMemberFacts {
    pub order: u32,
    pub name: String,
    pub unpacked_size: Option<u64>,
    pub data_crc32: Option<u32>,
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
    pub data_offset: u64,
    pub data_size: u64,
    pub compression_method: u8,
    pub compression_version: u8,
    pub compression_solid: bool,
    pub dict_size: u64,
    pub use_hash_mac: bool,
    pub redirection_type: Option<u64>,
    pub redirection_target: Option<String>,
    pub redirection_target_is_directory: bool,
}

/// A service-header record from one physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeServiceFacts {
    pub order: u32,
    pub name: String,
    #[serde(default)]
    pub subtype: Option<u16>,
    #[serde(default)]
    pub level: Option<u8>,
    pub unpacked_size: Option<u64>,
    pub data_crc32: Option<u32>,
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
    pub data_offset: u64,
    pub data_size: u64,
    pub compression_method: u8,
    pub compression_version: u8,
    pub compression_solid: bool,
    pub dict_size: u64,
    pub use_hash_mac: bool,
    #[serde(default)]
    pub stream_name: Option<String>,
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

impl RarVolumeServiceFacts {
    fn from_rar4_service(order: usize, fh: &crate::rar4::types::Rar4FileHeader) -> Self {
        let file_header = RarArchive::rar4_to_file_header(fh, false);
        let (packed_crc32, packed_blake2_hash) = split_packed_hash(
            RarArchive::packed_hash_for_split_segment(&file_header, None),
        );
        Self {
            order: order as u32,
            name: fh.name.clone(),
            subtype: None,
            level: None,
            unpacked_size: fh.unpacked_size,
            data_crc32: Some(fh.crc32),
            comment_crc16: None,
            packed_crc32,
            packed_blake2_hash,
            packed_hash_uses_mac: false,
            split_before: fh.split_before,
            split_after: fh.split_after,
            is_encrypted: fh.is_encrypted,
            data_offset: fh.data_offset,
            data_size: fh.packed_size,
            compression_method: file_header.compression.method.code(),
            compression_version: fh.unpack_version,
            compression_solid: file_header.compression.solid,
            dict_size: fh.dict_size,
            use_hash_mac: false,
            stream_name: None,
        }
    }

    fn from_rar4_comment(order: usize, comment: &crate::rar4::types::Rar4CommentHeader) -> Self {
        Self {
            order: order as u32,
            name: "CMT".to_string(),
            subtype: None,
            level: None,
            unpacked_size: Some(u64::from(comment.unpacked_size)),
            data_crc32: None,
            comment_crc16: Some(comment.crc16),
            packed_crc32: None,
            packed_blake2_hash: None,
            packed_hash_uses_mac: false,
            split_before: false,
            split_after: false,
            is_encrypted: false,
            data_offset: comment.data_offset,
            data_size: comment.packed_size,
            compression_method: rar4_method_to_compression_method(comment.method).code(),
            compression_version: comment.unpack_version,
            compression_solid: false,
            dict_size: 0,
            use_hash_mac: false,
            stream_name: None,
        }
    }

    fn from_rar4_old_service(
        order: usize,
        service: &crate::rar4::types::Rar4OldServiceHeader,
    ) -> Self {
        use crate::rar4::types::Rar4OldServiceData;

        let (name, unpacked_size, data_crc32, compression_method, compression_version, stream_name) =
            match &service.data {
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
                ),
                Rar4OldServiceData::Stream {
                    unpacked_size,
                    unpack_version,
                    method,
                    crc32,
                    stream_name,
                } => (
                    "STM".to_string(),
                    Some(u64::from(*unpacked_size)),
                    Some(*crc32),
                    rar4_method_to_compression_method(*method).code(),
                    *unpack_version,
                    Some(stream_name.clone()),
                ),
                Rar4OldServiceData::Unknown => (
                    format!("0x{:04x}", service.subtype),
                    None,
                    None,
                    CompressionMethod::Store.code(),
                    0,
                    None,
                ),
            };

        Self {
            order: order as u32,
            name,
            subtype: Some(service.subtype),
            level: Some(service.level),
            unpacked_size,
            data_crc32,
            comment_crc16: None,
            packed_crc32: None,
            packed_blake2_hash: None,
            packed_hash_uses_mac: false,
            split_before: false,
            split_after: false,
            is_encrypted: false,
            data_offset: service.data_offset,
            data_size: service.data_size,
            compression_method,
            compression_version,
            compression_solid: false,
            dict_size: 0x10000,
            use_hash_mac: false,
            stream_name,
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
            subtype: None,
            level: None,
            unpacked_size: service.header.inner.unpacked_size,
            data_crc32: service.header.inner.data_crc32,
            comment_crc16: None,
            packed_crc32,
            packed_blake2_hash,
            packed_hash_uses_mac,
            split_before: service.header.inner.split_before,
            split_after: service.header.inner.split_after,
            is_encrypted: service.is_encrypted,
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
                        unpacked_size: fh.unpacked_size,
                        data_crc32: (!fh.is_rar14).then_some(fh.crc32),
                        packed_crc32,
                        packed_blake2_hash,
                        packed_hash_uses_mac: false,
                        split_before: fh.split_before,
                        split_after: fh.split_after,
                        is_directory: fh.is_directory,
                        is_encrypted: fh.is_encrypted,
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
                members,
                services,
            });
        }

        let parsed = crate::header::parse_all_headers(&mut reader, password)?;
        let volume_number = parsed
            .main
            .as_ref()
            .and_then(|main| main.volume_number)
            .unwrap_or(0) as u32;
        let more_volumes = parsed.end.as_ref().is_some_and(|end| end.more_volumes);
        let is_solid = parsed.main.as_ref().is_some_and(|main| main.is_solid);
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
                    unpacked_size: parsed_file.header.unpacked_size,
                    data_crc32: parsed_file.header.data_crc32,
                    packed_crc32,
                    packed_blake2_hash,
                    packed_hash_uses_mac,
                    split_before: parsed_file.header.split_before,
                    split_after: parsed_file.header.split_after,
                    is_directory: parsed_file.header.is_directory,
                    is_encrypted: parsed_file.is_encrypted,
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
            members,
            services,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(decoded.members.len(), 1);
        assert_eq!(decoded.members[0].packed_crc32, None);
        assert_eq!(decoded.members[0].packed_blake2_hash, None);
        assert!(!decoded.members[0].packed_hash_uses_mac);
    }
}
