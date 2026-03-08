//! General header structure and CRC32 validation for RAR5.
//!
//! Every RAR5 block has:
//! - Header CRC32 (uint32, little-endian) covering everything from header size onward
//! - Header size (vint) - bytes from header type through end of extra area
//! - Header type (vint)
//! - Header flags (vint)
//! - Extra area size (vint, if flag 0x0001)
//! - Data area size (vint, if flag 0x0002)
//! - Type-specific fields
//! - Extra area

use std::io::{Read, Seek, SeekFrom};

use crate::error::{RarError, RarResult};
use crate::vint;

/// Common header flags shared by all header types.
pub mod flags {
    /// Extra area is present after type-specific fields.
    pub const EXTRA_AREA: u64 = 0x0001;
    /// Data area follows the header.
    pub const DATA_AREA: u64 = 0x0002;
    /// Skip this block if header type is unknown (rather than error).
    pub const SKIP_IF_UNKNOWN: u64 = 0x0004;
    /// Data continues from previous volume.
    pub const SPLIT_BEFORE: u64 = 0x0008;
    /// Data continues in next volume.
    pub const SPLIT_AFTER: u64 = 0x0010;
}

/// RAR5 header types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderType {
    MainArchive,
    File,
    Service,
    Encryption,
    EndArchive,
    Unknown(u64),
}

impl From<u64> for HeaderType {
    fn from(value: u64) -> Self {
        match value {
            1 => HeaderType::MainArchive,
            2 => HeaderType::File,
            3 => HeaderType::Service,
            4 => HeaderType::Encryption,
            5 => HeaderType::EndArchive,
            other => HeaderType::Unknown(other),
        }
    }
}

/// Raw parsed common header fields before dispatching to type-specific parsers.
#[derive(Debug, Clone)]
pub struct RawHeader {
    /// Offset of this header in the stream (position of the CRC32 field).
    pub offset: u64,
    /// CRC32 stored in the header.
    pub crc32: u32,
    /// Header size (bytes from header type to end of extra area).
    pub header_size: u64,
    /// Size of the header_size vint itself (needed for CRC range calculation).
    pub header_size_vint_len: usize,
    /// Header type.
    pub header_type: HeaderType,
    /// Header flags.
    pub flags: u64,
    /// Extra area size (0 if not present).
    pub extra_area_size: u64,
    /// Data area size (0 if not present).
    pub data_area_size: u64,
    /// The raw body bytes (from header type to end of extra area, i.e. header_size bytes).
    /// This is used for type-specific parsing and CRC validation.
    pub body: Vec<u8>,
}

impl RawHeader {
    /// Returns true if data continues from the previous volume.
    pub fn is_split_before(&self) -> bool {
        self.flags & flags::SPLIT_BEFORE != 0
    }

    /// Returns true if data continues in the next volume.
    pub fn is_split_after(&self) -> bool {
        self.flags & flags::SPLIT_AFTER != 0
    }

    /// Returns true if extra area is present.
    pub fn has_extra_area(&self) -> bool {
        self.flags & flags::EXTRA_AREA != 0
    }

    /// Returns true if data area is present.
    pub fn has_data_area(&self) -> bool {
        self.flags & flags::DATA_AREA != 0
    }
}

/// Read and validate the next raw header from the stream.
///
/// Returns `None` at EOF (no more headers).
pub fn read_raw_header<R: Read + Seek>(reader: &mut R) -> RarResult<Option<RawHeader>> {
    let offset = reader.stream_position().map_err(RarError::Io)?;

    // Read CRC32 (4 bytes little-endian)
    let mut crc_buf = [0u8; 4];
    match reader.read_exact(&mut crc_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RarError::Io(e)),
    }
    let stored_crc = u32::from_le_bytes(crc_buf);

    // Read header size vint
    let (header_size, header_size_vint_len) = vint::read_vint_from(reader)?;

    if header_size == 0 {
        return Err(RarError::CorruptArchive {
            detail: format!("zero header size at offset {offset}"),
        });
    }

    // Cap header body allocation to prevent resource exhaustion.
    // 16 MB is far larger than any legitimate RAR5 header.
    const MAX_HEADER_BODY: u64 = 16 * 1024 * 1024;
    if header_size > MAX_HEADER_BODY {
        return Err(RarError::CorruptArchive {
            detail: format!("header size {} exceeds maximum {}", header_size, MAX_HEADER_BODY),
        });
    }

    // Read the full header body (header_size bytes: type + flags + extras + type-specific)
    let body_len = header_size as usize;
    let mut body = vec![0u8; body_len];
    reader.read_exact(&mut body).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            RarError::TruncatedHeader { offset }
        } else {
            RarError::Io(e)
        }
    })?;

    // Validate CRC32 over header_size_vint + body
    let computed_crc = {
        let mut hasher = crc32fast::Hasher::new();
        // CRC covers the header size vint encoding and then the body
        let header_size_encoded = vint_encode_for_crc(header_size, header_size_vint_len);
        hasher.update(&header_size_encoded);
        hasher.update(&body);
        hasher.finalize()
    };

    if computed_crc != stored_crc {
        return Err(RarError::HeaderCrcMismatch {
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    // Parse common fields from body
    let mut pos = 0;
    let (header_type_val, n) = vint::read_vint(&body[pos..])?;
    pos += n;

    let (header_flags, n) = vint::read_vint(&body[pos..])?;
    pos += n;

    let extra_area_size = if header_flags & flags::EXTRA_AREA != 0 {
        let (size, n) = vint::read_vint(&body[pos..])?;
        pos += n;
        let _ = pos; // suppress unused warning
        size
    } else {
        0
    };

    let data_area_size = if header_flags & flags::DATA_AREA != 0 {
        let (size, n) = vint::read_vint(&body[pos..])?;
        pos += n;
        let _ = pos;
        size
    } else {
        0
    };

    Ok(Some(RawHeader {
        offset,
        crc32: stored_crc,
        header_size,
        header_size_vint_len,
        header_type: HeaderType::from(header_type_val),
        flags: header_flags,
        extra_area_size,
        data_area_size,
        body,
    }))
}

/// Skip over the data area of a header.
pub fn skip_data_area<R: Read + Seek>(reader: &mut R, raw: &RawHeader) -> RarResult<()> {
    if raw.data_area_size > 0 {
        if raw.data_area_size > i64::MAX as u64 {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "data area size {} exceeds seekable range",
                    raw.data_area_size
                ),
            });
        }
        reader
            .seek(SeekFrom::Current(raw.data_area_size as i64))
            .map_err(RarError::Io)?;
    }
    Ok(())
}

/// Re-encode a vint value to the exact number of bytes for CRC calculation.
fn vint_encode_for_crc(value: u64, byte_count: usize) -> Vec<u8> {
    // We need to produce exactly `byte_count` bytes encoding `value`.
    // Leading 0x80 padding bytes are allowed.
    let mut result = Vec::with_capacity(byte_count);
    let mut v = value;
    for i in 0..byte_count {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if i < byte_count - 1 {
            byte |= 0x80;
        }
        result.push(byte);
    }
    result
}

/// Get the byte position within the body where type-specific fields start.
/// This skips past header_type, header_flags, extra_area_size, and data_area_size vints.
pub fn type_specific_offset(raw: &RawHeader) -> RarResult<usize> {
    let mut pos = 0;
    // header type
    let (_, n) = vint::read_vint(&raw.body[pos..])?;
    pos += n;
    // header flags
    let (flags, n) = vint::read_vint(&raw.body[pos..])?;
    pos += n;
    // extra area size
    if flags & flags::EXTRA_AREA != 0 {
        let (_, n) = vint::read_vint(&raw.body[pos..])?;
        pos += n;
    }
    // data area size
    if flags & flags::DATA_AREA != 0 {
        let (_, n) = vint::read_vint(&raw.body[pos..])?;
        pos += n;
    }
    Ok(pos)
}

/// Extract the extra area bytes from the body, if present.
pub fn extra_area_bytes(raw: &RawHeader) -> Option<&[u8]> {
    if raw.extra_area_size == 0 {
        return None;
    }
    let ea_size = raw.extra_area_size as usize;
    let body_len = raw.body.len();
    if ea_size > body_len {
        return None;
    }
    Some(&raw.body[body_len - ea_size..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_header(header_type: u64, header_flags: u64, type_body: &[u8]) -> Vec<u8> {
        // Build the body: type vint + flags vint + type-specific
        let mut body = Vec::new();
        body.extend_from_slice(&crate::vint::encode_vint(header_type));
        body.extend_from_slice(&crate::vint::encode_vint(header_flags));
        body.extend_from_slice(type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = crate::vint::encode_vint(header_size);

        // Compute CRC32 over header_size_bytes + body
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_size_bytes);
        hasher.update(&body);
        let crc = hasher.finalize();

        // Build full header: CRC32 LE + header_size vint + body
        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    #[test]
    fn test_read_raw_header_basic() {
        let data = build_header(1, 0, &[]);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        assert_eq!(raw.header_type, HeaderType::MainArchive);
        assert_eq!(raw.flags, 0);
        assert_eq!(raw.extra_area_size, 0);
        assert_eq!(raw.data_area_size, 0);
    }

    #[test]
    fn test_read_raw_header_with_data_flag() {
        // flags = DATA_AREA (0x02), data_area_size = 100
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&crate::vint::encode_vint(100)); // data_area_size
        let data = build_header(2, flags::DATA_AREA, &type_body);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        assert_eq!(raw.header_type, HeaderType::File);
        assert_eq!(raw.data_area_size, 100);
    }

    #[test]
    fn test_crc_mismatch() {
        let mut data = build_header(1, 0, &[]);
        // Corrupt a body byte
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        let mut cursor = std::io::Cursor::new(data);
        let result = read_raw_header(&mut cursor);
        assert!(matches!(result, Err(RarError::HeaderCrcMismatch { .. })));
    }

    #[test]
    fn test_eof_returns_none() {
        let data: &[u8] = &[];
        let mut cursor = std::io::Cursor::new(data);
        let result = read_raw_header(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_huge_header_rejected() {
        // Construct a header with header_size > 16 MB.
        // We can't easily build the full body, but we can craft bytes where
        // the header_size vint decodes to a huge value.
        // Build: CRC32(header_size_vint + body) | header_size_vint | body
        // We need the CRC to match so read_raw_header reaches the size check.
        // Instead, just encode a vint for a large header_size and provide
        // a valid CRC over the (vint + fake body). But we don't need the body
        // to be present -- the size check happens before the read.
        //
        // Actually, the CRC check happens after reading the body, so let's
        // just test that the error fires by providing a vint that decodes
        // to > 16 MB. The read_exact will fail with TruncatedHeader or the
        // size guard will fire first.
        //
        // The size guard fires BEFORE the allocation/read, so we just need:
        // CRC32 (4 bytes) + vint encoding of 17_000_000

        let huge_size: u64 = 17_000_000;
        let header_size_bytes = crate::vint::encode_vint(huge_size);

        // CRC doesn't matter since we'll error before checking it
        let fake_crc: u32 = 0;
        let mut data = Vec::new();
        data.extend_from_slice(&fake_crc.to_le_bytes());
        data.extend_from_slice(&header_size_bytes);

        let mut cursor = std::io::Cursor::new(data);
        let result = read_raw_header(&mut cursor);
        assert!(
            matches!(result, Err(RarError::CorruptArchive { ref detail }) if detail.contains("exceeds maximum")),
            "expected CorruptArchive error for huge header, got: {result:?}"
        );
    }

    #[test]
    fn test_header_type_mapping() {
        assert_eq!(HeaderType::from(1), HeaderType::MainArchive);
        assert_eq!(HeaderType::from(2), HeaderType::File);
        assert_eq!(HeaderType::from(3), HeaderType::Service);
        assert_eq!(HeaderType::from(4), HeaderType::Encryption);
        assert_eq!(HeaderType::from(5), HeaderType::EndArchive);
        assert_eq!(HeaderType::from(99), HeaderType::Unknown(99));
    }
}
