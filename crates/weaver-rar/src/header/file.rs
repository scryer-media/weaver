//! File header (type 2) parsing.
//!
//! File-specific flags:
//! - 0x0001: Directory
//! - 0x0002: Unix mtime present (uint32)
//! - 0x0004: CRC32 present (uint32)
//! - 0x0008: Unpacked size unknown
//!
//! Fields in order:
//! - Unpacked size (vint)
//! - Attributes (vint)
//! - mtime (uint32 Unix timestamp, if flag 0x0002)
//! - Data CRC32 (uint32, if flag 0x0004)
//! - Compression info (vint, bit-packed)
//! - Host OS (vint)
//! - Name length (vint)
//! - Name (UTF-8, name_length bytes)

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{RarError, RarResult};
use crate::header::common::{self, RawHeader};
use crate::header::compression_info;
use crate::types::{CompressionInfo, FileAttributes, HostOs};
use crate::vint;

/// File-specific flags.
pub mod flags {
    /// Entry is a directory.
    pub const DIRECTORY: u64 = 0x0001;
    /// Unix mtime is present as uint32.
    pub const TIME_PRESENT: u64 = 0x0002;
    /// CRC32 of unpacked data is present.
    pub const CRC32_PRESENT: u64 = 0x0004;
    /// Unpacked size is unknown.
    pub const UNPACKED_SIZE_UNKNOWN: u64 = 0x0008;
}

/// Parsed file header.
#[derive(Debug, Clone)]
pub struct FileHeader {
    /// File name (UTF-8).
    pub name: String,
    /// Unpacked size in bytes (None if unknown).
    pub unpacked_size: Option<u64>,
    /// File attributes.
    pub attributes: FileAttributes,
    /// Modification time.
    pub mtime: Option<SystemTime>,
    /// CRC32 of unpacked data.
    pub data_crc32: Option<u32>,
    /// Compression information.
    pub compression: CompressionInfo,
    /// Host OS.
    pub host_os: HostOs,
    /// Is this a directory entry.
    pub is_directory: bool,
    /// File-specific flags (raw).
    pub file_flags: u64,
    /// Data area size from the common header.
    pub data_size: u64,
    /// Data continues from previous volume.
    pub split_before: bool,
    /// Data continues in next volume.
    pub split_after: bool,
    /// Offset where the data area begins in the stream.
    pub data_offset: u64,
    /// Whether this file's data is encrypted.
    pub is_encrypted: bool,
}

/// Parse a file header from a raw header.
///
/// `data_offset` is the stream position where the data area begins
/// (right after the header).
pub fn parse(raw: &RawHeader, data_offset: u64) -> RarResult<FileHeader> {
    let offset = common::type_specific_offset(raw)?;
    let body = &raw.body[offset..];

    // Separate out extra area from end of body
    let ea_size = raw.extra_area_size as usize;
    let type_fields_len = body.len().saturating_sub(ea_size);
    let type_fields = &body[..type_fields_len];

    let mut pos = 0;

    // File-specific flags
    let (file_flags, n) = vint::read_vint(&type_fields[pos..])?;
    pos += n;

    let is_directory = file_flags & flags::DIRECTORY != 0;
    let size_unknown = file_flags & flags::UNPACKED_SIZE_UNKNOWN != 0;

    // Unpacked size
    let (raw_unpacked_size, n) = vint::read_vint(&type_fields[pos..])?;
    pos += n;
    let unpacked_size = if size_unknown {
        None
    } else {
        Some(raw_unpacked_size)
    };

    // Attributes
    let (attrs, n) = vint::read_vint(&type_fields[pos..])?;
    pos += n;

    // mtime (uint32 Unix timestamp)
    let mtime = if file_flags & flags::TIME_PRESENT != 0 {
        if pos + 4 > type_fields.len() {
            return Err(RarError::TruncatedHeader { offset: raw.offset });
        }
        let ts = u32::from_le_bytes(type_fields[pos..pos + 4].try_into().unwrap());
        pos += 4;
        Some(UNIX_EPOCH + Duration::from_secs(ts as u64))
    } else {
        None
    };

    // Data CRC32 (uint32)
    let data_crc32 = if file_flags & flags::CRC32_PRESENT != 0 {
        if pos + 4 > type_fields.len() {
            return Err(RarError::TruncatedHeader { offset: raw.offset });
        }
        let crc = u32::from_le_bytes(type_fields[pos..pos + 4].try_into().unwrap());
        pos += 4;
        Some(crc)
    } else {
        None
    };

    // Compression info (vint, bit-packed)
    let (comp_raw, n) = vint::read_vint(&type_fields[pos..])?;
    pos += n;
    let compression = compression_info::decode_compression_info(comp_raw);

    // Host OS
    let (os_val, n) = vint::read_vint(&type_fields[pos..])?;
    pos += n;

    // Name length
    let (name_len, n) = vint::read_vint(&type_fields[pos..])?;
    pos += n;

    // Name (UTF-8)
    let name_end = pos + name_len as usize;
    if name_end > type_fields.len() {
        return Err(RarError::TruncatedHeader { offset: raw.offset });
    }
    let name = String::from_utf8_lossy(&type_fields[pos..name_end]).into_owned();

    Ok(FileHeader {
        name,
        unpacked_size,
        attributes: FileAttributes(attrs),
        mtime,
        data_crc32,
        compression,
        host_os: HostOs::from(os_val),
        is_directory,
        file_flags,
        data_size: raw.data_area_size,
        split_before: raw.is_split_before(),
        split_after: raw.is_split_after(),
        data_offset,
        is_encrypted: false, // Set by caller from extra records or RAR4 flags
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::common::read_raw_header;
    use crate::vint::encode_vint;

    struct TestFileHeaderArgs<'a> {
        file_flags: u64,
        unpacked_size: u64,
        attributes: u64,
        mtime: Option<u32>,
        crc32: Option<u32>,
        comp_info: u64,
        host_os: u64,
        name: &'a str,
    }

    fn build_file_header(args: TestFileHeaderArgs<'_>) -> Vec<u8> {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(args.file_flags));
        type_body.extend_from_slice(&encode_vint(args.unpacked_size));
        type_body.extend_from_slice(&encode_vint(args.attributes));
        if let Some(ts) = args.mtime {
            type_body.extend_from_slice(&ts.to_le_bytes());
        }
        if let Some(crc) = args.crc32 {
            type_body.extend_from_slice(&crc.to_le_bytes());
        }
        type_body.extend_from_slice(&encode_vint(args.comp_info));
        type_body.extend_from_slice(&encode_vint(args.host_os));
        type_body.extend_from_slice(&encode_vint(args.name.len() as u64));
        type_body.extend_from_slice(args.name.as_bytes());

        let header_type = 2u64;
        let common_flags = 0u64;

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(common_flags));
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
    fn test_basic_file_header() {
        let data = build_file_header(TestFileHeaderArgs {
            file_flags: flags::TIME_PRESENT | flags::CRC32_PRESENT,
            unpacked_size: 1024,
            attributes: 0o644,
            mtime: Some(1700000000),
            crc32: Some(0xDEADBEEF),
            comp_info: 0, // store, version 0, dict 128KB
            host_os: 1,   // Unix
            name: "test.txt",
        });
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let data_offset = cursor.position();
        let fh = parse(&raw, data_offset).unwrap();

        assert_eq!(fh.name, "test.txt");
        assert_eq!(fh.unpacked_size, Some(1024));
        assert!(!fh.is_directory);
        assert_eq!(fh.data_crc32, Some(0xDEADBEEF));
        assert_eq!(
            fh.compression.method,
            crate::types::CompressionMethod::Store
        );
        assert_eq!(fh.host_os, HostOs::Unix);
        assert!(fh.mtime.is_some());
    }

    #[test]
    fn test_directory_entry() {
        let data = build_file_header(TestFileHeaderArgs {
            file_flags: flags::DIRECTORY,
            unpacked_size: 0,
            attributes: 0o755,
            mtime: None,
            crc32: None,
            comp_info: 0,
            host_os: 1,
            name: "subdir/",
        });
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let data_offset = cursor.position();
        let fh = parse(&raw, data_offset).unwrap();

        assert!(fh.is_directory);
        assert_eq!(fh.name, "subdir/");
        assert!(fh.mtime.is_none());
        assert!(fh.data_crc32.is_none());
    }

    #[test]
    fn test_unknown_size() {
        let data = build_file_header(TestFileHeaderArgs {
            file_flags: flags::UNPACKED_SIZE_UNKNOWN,
            unpacked_size: 0xFFFFFFFFFFFFFFFF,
            attributes: 0,
            mtime: None,
            crc32: None,
            comp_info: 0,
            host_os: 0,
            name: "stream.dat",
        });
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let data_offset = cursor.position();
        let fh = parse(&raw, data_offset).unwrap();

        assert!(fh.unpacked_size.is_none());
    }
}
