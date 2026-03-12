use serde::{Deserialize, Serialize};

use super::{RarArchive, ReadSeek};
use crate::error::RarResult;
use crate::signature;
use crate::types::ArchiveFormat;

/// Immutable header facts parsed from a single physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeFacts {
    pub format: u8,
    pub volume_number: u32,
    pub more_volumes: bool,
    pub is_solid: bool,
    pub is_encrypted: bool,
    pub members: Vec<RarVolumeMemberFacts>,
}

/// A single ordered file-header record from one physical RAR volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RarVolumeMemberFacts {
    pub order: u32,
    pub name: String,
    pub unpacked_size: Option<u64>,
    pub data_crc32: Option<u32>,
    pub split_before: bool,
    pub split_after: bool,
    pub is_directory: bool,
    pub is_encrypted: bool,
    pub data_offset: u64,
    pub data_size: u64,
    pub compression_method: u8,
    pub compression_solid: bool,
    pub use_hash_mac: bool,
}

impl RarVolumeFacts {
    pub fn archive_format(&self) -> ArchiveFormat {
        match self.format {
            4 => ArchiveFormat::Rar4,
            _ => ArchiveFormat::Rar5,
        }
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

        if format == ArchiveFormat::Rar4 {
            let parsed = crate::rar4::parse_rar4_headers(&mut reader)?;
            let volume_number = parsed
                .end
                .as_ref()
                .and_then(|end| end.volume_number)
                .map(|value| value as u32)
                .unwrap_or(0);
            let more_volumes = parsed.end.as_ref().is_some_and(|end| end.more_volumes);
            let members = parsed
                .files
                .iter()
                .enumerate()
                .map(|(order, fh)| RarVolumeMemberFacts {
                    order: order as u32,
                    name: fh.name.clone(),
                    unpacked_size: Some(fh.unpacked_size),
                    data_crc32: Some(fh.crc32),
                    split_before: fh.split_before,
                    split_after: fh.split_after,
                    is_directory: fh.is_directory,
                    is_encrypted: fh.is_encrypted,
                    data_offset: fh.data_offset,
                    data_size: fh.packed_size,
                    compression_method: RarArchive::rar4_to_file_header(fh)
                        .compression
                        .method
                        .code(),
                    compression_solid: fh.is_solid,
                    use_hash_mac: false,
                })
                .collect();
            return Ok(RarVolumeFacts {
                format: 4,
                volume_number,
                more_volumes,
                is_solid: parsed.archive_header.is_solid,
                is_encrypted: parsed.archive_header.is_encrypted,
                members,
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
        let members = parsed
            .files
            .into_iter()
            .enumerate()
            .map(|(order, parsed_file)| RarVolumeMemberFacts {
                order: order as u32,
                name: parsed_file.header.name,
                unpacked_size: parsed_file.header.unpacked_size,
                data_crc32: parsed_file.header.data_crc32,
                split_before: parsed_file.header.split_before,
                split_after: parsed_file.header.split_after,
                is_directory: parsed_file.header.is_directory,
                is_encrypted: parsed_file.is_encrypted,
                data_offset: parsed_file.header.data_offset,
                data_size: parsed_file.header.data_size,
                compression_method: parsed_file.header.compression.method.code(),
                compression_solid: parsed_file.header.compression.solid,
                use_hash_mac: parsed_file
                    .file_encryption
                    .as_ref()
                    .is_some_and(|info| info.use_hash_mac),
            })
            .collect();

        Ok(RarVolumeFacts {
            format: 5,
            volume_number,
            more_volumes,
            is_solid,
            is_encrypted: parsed.is_encrypted,
            members,
        })
    }
}
