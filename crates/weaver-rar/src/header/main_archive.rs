//! Main archive header (type 1) parsing.
//!
//! Archive-specific flags:
//! - 0x0001: Volume (multi-volume set)
//! - 0x0002: Volume number field present
//! - 0x0004: Solid archive
//! - 0x0008: Recovery record present
//! - 0x0010: Locked archive

use crate::error::RarResult;
use crate::header::common::{self, RawHeader};
use crate::vint;

/// Main archive header flags.
pub mod flags {
    /// Multi-volume archive.
    pub const VOLUME: u64 = 0x0001;
    /// Volume number field is present (all volumes except first).
    pub const VOLUME_NUMBER: u64 = 0x0002;
    /// Solid archive.
    pub const SOLID: u64 = 0x0004;
    /// Recovery record present.
    pub const RECOVERY_RECORD: u64 = 0x0008;
    /// Locked archive.
    pub const LOCKED: u64 = 0x0010;
}

/// Parsed main archive header.
#[derive(Debug, Clone)]
pub struct MainArchiveHeader {
    /// Whether this is a multi-volume archive.
    pub is_volume: bool,
    /// Volume number (0-based, None for first volume or non-volume archives).
    pub volume_number: Option<u64>,
    /// Whether this is a solid archive.
    pub is_solid: bool,
    /// Whether a recovery record is present.
    pub has_recovery_record: bool,
    /// Whether the archive is locked.
    pub is_locked: bool,
    /// Archive-specific flags (raw).
    pub archive_flags: u64,
}

/// Parse a main archive header from a raw header.
pub fn parse(raw: &RawHeader) -> RarResult<MainArchiveHeader> {
    let offset = common::type_specific_offset(raw)?;
    let body = &raw.body[offset..];

    let mut pos = 0;

    // Read archive-specific flags
    let (archive_flags, n) = vint::read_vint(&body[pos..])?;
    pos += n;

    let is_volume = archive_flags & flags::VOLUME != 0;
    let is_solid = archive_flags & flags::SOLID != 0;
    let has_recovery_record = archive_flags & flags::RECOVERY_RECORD != 0;
    let is_locked = archive_flags & flags::LOCKED != 0;

    let volume_number = if archive_flags & flags::VOLUME_NUMBER != 0 {
        let (vn, n) = vint::read_vint(&body[pos..])?;
        pos += n;
        let _ = pos;
        Some(vn)
    } else {
        None
    };

    Ok(MainArchiveHeader {
        is_volume,
        volume_number,
        is_solid,
        has_recovery_record,
        is_locked,
        archive_flags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::common::read_raw_header;
    use crate::vint::encode_vint;

    fn build_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
        // Build type-specific body: archive_flags + optional volume_number
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(archive_flags));
        if let Some(vn) = volume_number {
            type_body.extend_from_slice(&encode_vint(vn));
        }

        // Build full header using common builder
        let header_type = 1u64; // MainArchive
        let header_flags = 0u64;

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(header_flags));
        body.extend_from_slice(&type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_vint(header_size);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_size_bytes);
        hasher.update(&body);
        let crc = hasher.finalize();

        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    #[test]
    fn test_simple_main_header() {
        let data = build_main_header(0, None);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();
        assert!(!main.is_volume);
        assert!(!main.is_solid);
        assert!(main.volume_number.is_none());
    }

    #[test]
    fn test_volume_header() {
        let data = build_main_header(flags::VOLUME | flags::VOLUME_NUMBER, Some(3));
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();
        assert!(main.is_volume);
        assert_eq!(main.volume_number, Some(3));
    }

    #[test]
    fn test_solid_header() {
        let data = build_main_header(flags::SOLID, None);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();
        assert!(main.is_solid);
    }
}
