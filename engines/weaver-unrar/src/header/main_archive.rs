//! Main archive header (type 1) parsing.
//!
//! Archive-specific flags:
//! - 0x0001: Volume (multi-volume set)
//! - 0x0002: Volume number field present
//! - 0x0004: Solid archive
//! - 0x0008: Recovery record present
//! - 0x0010: Locked archive

use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    /// Whether a main-header locator extra record was present.
    pub has_locator: bool,
    /// Absolute offset of the quick-open block, if advertised.
    pub quick_open_offset: Option<u64>,
    /// Absolute offset of the embedded recovery record, if advertised.
    pub recovery_record_offset: Option<u64>,
    /// Original archive name from the metadata extra record.
    pub original_name: Option<String>,
    /// Original archive creation time from the metadata extra record.
    pub original_creation_time: Option<SystemTime>,
}

mod main_extra {
    pub const LOCATOR: u64 = 0x01;
    pub const METADATA: u64 = 0x02;

    pub const LOCATOR_QLIST: u64 = 0x01;
    pub const LOCATOR_RR: u64 = 0x02;

    pub const METADATA_NAME: u64 = 0x01;
    pub const METADATA_CTIME: u64 = 0x02;
    pub const METADATA_UNIXTIME: u64 = 0x04;
    pub const METADATA_UNIX_NS: u64 = 0x08;
}

const MAXPATHSIZE: u64 = 0x10000;
const WINDOWS_TICKS_PER_SECOND: u64 = 10_000_000;
const WINDOWS_TO_UNIX_SECONDS: u64 = 11_644_473_600;

#[derive(Debug, Clone, Default)]
struct MainExtraMetadata {
    has_locator: bool,
    quick_open_offset: Option<u64>,
    recovery_record_offset: Option<u64>,
    original_name: Option<String>,
    original_creation_time: Option<SystemTime>,
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

    let extras = parse_main_extra_records(raw);

    Ok(MainArchiveHeader {
        is_volume,
        volume_number,
        is_solid,
        has_recovery_record,
        is_locked,
        archive_flags,
        has_locator: extras.has_locator,
        quick_open_offset: extras.quick_open_offset,
        recovery_record_offset: extras.recovery_record_offset,
        original_name: extras.original_name,
        original_creation_time: extras.original_creation_time,
    })
}

fn parse_main_extra_records(raw: &RawHeader) -> MainExtraMetadata {
    let mut metadata = MainExtraMetadata::default();
    let Some(data) = common::extra_area_bytes(raw) else {
        return metadata;
    };

    let mut pos = 0usize;
    while pos < data.len() {
        let (record_size, size_n) = match vint::read_vint(&data[pos..]) {
            Ok(value) => value,
            Err(_) => break,
        };
        pos += size_n;
        if record_size == 0 {
            break;
        }
        let Ok(record_size) = usize::try_from(record_size) else {
            break;
        };
        let Some(record_end) = pos.checked_add(record_size) else {
            break;
        };
        if record_end > data.len() {
            break;
        }

        let record_data = &data[pos..record_end];
        let (record_type, type_n) = match vint::read_vint(record_data) {
            Ok(value) => value,
            Err(_) => break,
        };
        let record_body = &record_data[type_n..];
        match record_type {
            main_extra::LOCATOR => parse_locator_extra(record_body, raw.offset, &mut metadata),
            main_extra::METADATA => parse_metadata_extra(record_body, &mut metadata),
            _ => {}
        }

        pos = record_end;
    }

    metadata
}

fn parse_locator_extra(data: &[u8], block_offset: u64, metadata: &mut MainExtraMetadata) {
    metadata.has_locator = true;
    let (locator_flags, mut pos) = read_raw_vint_lossy(data);

    if locator_flags & main_extra::LOCATOR_QLIST != 0 {
        let (offset, n) = read_raw_vint_lossy(data.get(pos..).unwrap_or(&[]));
        pos = pos.saturating_add(n).min(data.len());
        if offset != 0 {
            metadata.quick_open_offset = block_offset.checked_add(offset);
        }
    }

    if locator_flags & main_extra::LOCATOR_RR != 0 {
        let (offset, _) = read_raw_vint_lossy(data.get(pos..).unwrap_or(&[]));
        if offset != 0 {
            metadata.recovery_record_offset = block_offset.checked_add(offset);
        }
    }
}

fn parse_metadata_extra(data: &[u8], metadata: &mut MainExtraMetadata) {
    let (metadata_flags, mut pos) = read_raw_vint_lossy(data);

    if metadata_flags & main_extra::METADATA_NAME != 0 {
        let (name_size, n) = read_raw_vint_lossy(data.get(pos..).unwrap_or(&[]));
        pos = pos.saturating_add(n).min(data.len());
        if name_size > 0 && name_size < MAXPATHSIZE {
            let Ok(name_size) = usize::try_from(name_size) else {
                return;
            };
            let mut name_bytes = vec![0u8; name_size];
            let available = data.get(pos..).unwrap_or(&[]);
            let copied = available.len().min(name_size);
            name_bytes[..copied].copy_from_slice(&available[..copied]);
            pos = pos.saturating_add(copied).min(data.len());
            if name_bytes.first().copied().unwrap_or(0) != 0 {
                metadata.original_name = Some(common::decode_utf8_prefix_until_nul(&name_bytes));
            }
        }
    }

    if metadata_flags & main_extra::METADATA_CTIME != 0 {
        metadata.original_creation_time = if metadata_flags & main_extra::METADATA_UNIXTIME != 0 {
            if metadata_flags & main_extra::METADATA_UNIX_NS != 0 {
                system_time_from_unix_nanos(read_raw_u64_lossy(data, &mut pos))
            } else {
                UNIX_EPOCH.checked_add(Duration::new(read_raw_u32_lossy(data, &mut pos) as u64, 0))
            }
        } else {
            system_time_from_windows(read_raw_u64_lossy(data, &mut pos))
        };
    }
}

fn read_raw_vint_lossy(data: &[u8]) -> (u64, usize) {
    let mut result = 0u64;
    let mut read = 0usize;
    for shift in (0..64).step_by(7) {
        let Some(&byte) = data.get(read) else {
            return (0, read);
        };
        read += 1;
        result += u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return (result, read);
        }
    }
    (0, read)
}

fn read_raw_u32_lossy(data: &[u8], pos: &mut usize) -> u32 {
    let Some(end) = pos.checked_add(4) else {
        return 0;
    };
    if end <= data.len() {
        let value = u32::from_le_bytes(data[*pos..end].try_into().unwrap_or([0; 4]));
        *pos = end;
        value
    } else {
        0
    }
}

fn read_raw_u64_lossy(data: &[u8], pos: &mut usize) -> u64 {
    let low = read_raw_u32_lossy(data, pos);
    let high = read_raw_u32_lossy(data, pos);
    u64::from(low) | (u64::from(high) << 32)
}

fn system_time_from_unix_nanos(nanos_since_epoch: u64) -> Option<SystemTime> {
    let seconds = nanos_since_epoch / 1_000_000_000;
    let nanos = (nanos_since_epoch % 1_000_000_000) as u32;
    UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
}

fn system_time_from_windows(filetime: u64) -> Option<SystemTime> {
    let seconds = filetime / WINDOWS_TICKS_PER_SECOND;
    let nanos = ((filetime % WINDOWS_TICKS_PER_SECOND) * 100) as u32;

    if seconds >= WINDOWS_TO_UNIX_SECONDS {
        UNIX_EPOCH.checked_add(Duration::new(seconds - WINDOWS_TO_UNIX_SECONDS, nanos))
    } else {
        UNIX_EPOCH.checked_sub(Duration::new(WINDOWS_TO_UNIX_SECONDS - seconds, nanos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::common::read_raw_header;
    use crate::vint::encode_vint;

    fn build_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
        build_main_header_with_extra(archive_flags, volume_number, &[])
    }

    fn build_main_header_with_extra(
        archive_flags: u64,
        volume_number: Option<u64>,
        extra_area: &[u8],
    ) -> Vec<u8> {
        // Build type-specific body: archive_flags + optional volume_number
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(archive_flags));
        if let Some(vn) = volume_number {
            type_body.extend_from_slice(&encode_vint(vn));
        }

        // Build full header using common builder
        let header_type = 1u64; // MainArchive
        let header_flags = if extra_area.is_empty() {
            0
        } else {
            common::flags::EXTRA_AREA
        };

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(header_flags));
        if !extra_area.is_empty() {
            body.extend_from_slice(&encode_vint(extra_area.len() as u64));
        }
        body.extend_from_slice(&type_body);
        body.extend_from_slice(extra_area);

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

    fn build_extra_record(record_type: u64, body: &[u8]) -> Vec<u8> {
        let type_bytes = encode_vint(record_type);
        let mut result = Vec::new();
        result.extend_from_slice(&encode_vint((type_bytes.len() + body.len()) as u64));
        result.extend_from_slice(&type_bytes);
        result.extend_from_slice(body);
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

    #[test]
    fn test_main_locator_extra_offsets_are_absolute() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(
            main_extra::LOCATOR_QLIST | main_extra::LOCATOR_RR,
        ));
        body.extend_from_slice(&encode_vint(120));
        body.extend_from_slice(&encode_vint(240));
        let extra = build_extra_record(main_extra::LOCATOR, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert!(main.has_locator);
        assert_eq!(main.quick_open_offset, Some(raw.offset + 120));
        assert_eq!(main.recovery_record_offset, Some(raw.offset + 240));
    }

    #[test]
    fn test_main_locator_zero_offsets_are_reserved_and_ignored() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(
            main_extra::LOCATOR_QLIST | main_extra::LOCATOR_RR,
        ));
        body.extend_from_slice(&encode_vint(0));
        body.extend_from_slice(&encode_vint(0));
        let extra = build_extra_record(main_extra::LOCATOR, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert!(main.has_locator);
        assert_eq!(main.quick_open_offset, None);
        assert_eq!(main.recovery_record_offset, None);
    }

    #[test]
    fn test_main_metadata_extra_name_and_unix_ns_ctime() {
        let unix_nanos = 1_700_000_000_123_456_789u64;
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(
            main_extra::METADATA_NAME
                | main_extra::METADATA_CTIME
                | main_extra::METADATA_UNIXTIME
                | main_extra::METADATA_UNIX_NS,
        ));
        body.extend_from_slice(&encode_vint(12));
        body.extend_from_slice(b"release.rar\0");
        body.extend_from_slice(&unix_nanos.to_le_bytes());
        let extra = build_extra_record(main_extra::METADATA, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert_eq!(main.original_name.as_deref(), Some("release.rar"));
        assert_eq!(
            main.original_creation_time,
            Some(UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789))
        );
    }

    #[test]
    fn test_main_metadata_name_starting_with_zero_is_reserved_and_ignored() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(main_extra::METADATA_NAME));
        body.extend_from_slice(&encode_vint(5));
        body.extend_from_slice(b"\0skip");
        let extra = build_extra_record(main_extra::METADATA, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert_eq!(main.original_name, None);
    }

    #[test]
    fn test_main_metadata_short_name_zero_fills_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(main_extra::METADATA_NAME));
        body.extend_from_slice(&encode_vint(12));
        body.extend_from_slice(b"rel");
        let extra = build_extra_record(main_extra::METADATA, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert_eq!(main.original_name.as_deref(), Some("rel"));
    }

    #[test]
    fn test_main_metadata_invalid_utf8_name_keeps_prefix_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(main_extra::METADATA_NAME));
        body.extend_from_slice(&encode_vint(15));
        body.extend_from_slice(b"release/\xfftail");
        let extra = build_extra_record(main_extra::METADATA, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert_eq!(main.original_name.as_deref(), Some("release/"));
    }

    #[test]
    fn test_main_metadata_overlong_utf8_name_decodes_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(main_extra::METADATA_NAME));
        body.extend_from_slice(&encode_vint(11));
        body.extend_from_slice(b"release\xc0\xafok");
        let extra = build_extra_record(main_extra::METADATA, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert_eq!(main.original_name.as_deref(), Some("release/ok"));
    }

    #[test]
    fn test_main_metadata_missing_unix_ctime_defaults_to_epoch_like_unrar() {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(
            main_extra::METADATA_CTIME | main_extra::METADATA_UNIXTIME,
        ));
        let extra = build_extra_record(main_extra::METADATA, &body);
        let data = build_main_header_with_extra(0, None, &extra);

        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let main = parse(&raw).unwrap();

        assert_eq!(main.original_creation_time, Some(UNIX_EPOCH));
    }
}
