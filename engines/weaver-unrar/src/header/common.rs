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

use tracing::warn;

use crate::error::{RarError, RarResult};
use crate::limits::UNRAR_RAR5_MAX_HEADER_BODY;
use crate::vint;

/// Maximum header body size accepted by the RAR5 parser.
///
/// Match UnRAR's `MAX_HEADER_SIZE_RAR5` (2 MiB). Keeping the same cap prevents
/// malformed header sizes from turning into large allocations before the parser
/// can reject the archive as corrupt.
pub(crate) const MAX_HEADER_BODY: u64 = UNRAR_RAR5_MAX_HEADER_BODY;
const MAX_HEADER_SIZE_VINT_BYTES: usize = 3;

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

/// Read and parse the next raw header from the stream.
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

    validate_header_size(offset, header_size, header_size_vint_len)?;

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
        warn!(
            "RAR5 header CRC mismatch at offset {}: expected {:#010x}, got {:#010x}; parsing anyway like UnRAR",
            offset, stored_crc, computed_crc
        );
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
pub(crate) fn vint_encode_for_crc(value: u64, byte_count: usize) -> Vec<u8> {
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

/// Decode RAR5 UTF-8 text the way UnRAR callers use `UtfToWide`.
///
/// The official decoder validates only continuation-byte shapes, so overlong
/// sequences are decoded instead of rejected. It returns `false` at the first
/// malformed byte, but several header readers ignore that return value and keep
/// the prefix decoded before the error. They also stop at the first NUL byte.
pub(crate) fn decode_utf8_prefix_until_nul(bytes: &[u8]) -> String {
    decode_utf8_prefix_until_nul_with_unrar_bytes(bytes).0
}

pub(crate) fn decode_utf8_prefix_until_nul_with_unrar_bytes(bytes: &[u8]) -> (String, Vec<u8>) {
    let mut decoded = String::new();
    let mut unrar_bytes = Vec::new();
    let mut pos = 0usize;

    while let Some(&first) = bytes.get(pos) {
        if first == 0 {
            break;
        }
        pos += 1;

        let scalar = if first < 0x80 {
            u32::from(first)
        } else if first >> 5 == 0b110 {
            let Some(&b1) = bytes.get(pos) else {
                break;
            };
            if b1 & 0xc0 != 0x80 {
                break;
            }
            pos += 1;
            (u32::from(first & 0x1f) << 6) | u32::from(b1 & 0x3f)
        } else if first >> 4 == 0b1110 {
            let (Some(&b1), Some(&b2)) = (bytes.get(pos), bytes.get(pos + 1)) else {
                break;
            };
            if b1 & 0xc0 != 0x80 || b2 & 0xc0 != 0x80 {
                break;
            }
            pos += 2;
            (u32::from(first & 0x0f) << 12) | (u32::from(b1 & 0x3f) << 6) | u32::from(b2 & 0x3f)
        } else if first >> 3 == 0b11110 {
            let (Some(&b1), Some(&b2), Some(&b3)) =
                (bytes.get(pos), bytes.get(pos + 1), bytes.get(pos + 2))
            else {
                break;
            };
            if b1 & 0xc0 != 0x80 || b2 & 0xc0 != 0x80 || b3 & 0xc0 != 0x80 {
                break;
            }
            pos += 3;
            (u32::from(first & 0x07) << 18)
                | (u32::from(b1 & 0x3f) << 12)
                | (u32::from(b2 & 0x3f) << 6)
                | u32::from(b3 & 0x3f)
        } else {
            break;
        };

        if scalar > 0x10ffff {
            continue;
        }
        let ch = if (0xd800..=0xdfff).contains(&scalar) {
            char::REPLACEMENT_CHARACTER
        } else {
            let Some(ch) = char::from_u32(scalar) else {
                continue;
            };
            ch
        };
        decoded.push(ch);
        append_unrar_utf8(scalar, &mut unrar_bytes);
    }

    (decoded, unrar_bytes)
}

fn append_unrar_utf8(scalar: u32, out: &mut Vec<u8>) {
    if scalar <= 0x7f {
        out.push(scalar as u8);
    } else if scalar <= 0x7ff {
        out.push((0xc0 | ((scalar >> 6) & 0x1f)) as u8);
        out.push((0x80 | (scalar & 0x3f)) as u8);
    } else if scalar <= 0xffff {
        out.push((0xe0 | ((scalar >> 12) & 0x0f)) as u8);
        out.push((0x80 | ((scalar >> 6) & 0x3f)) as u8);
        out.push((0x80 | (scalar & 0x3f)) as u8);
    } else if scalar <= 0x10ffff {
        out.push((0xf0 | ((scalar >> 18) & 0x07)) as u8);
        out.push((0x80 | ((scalar >> 12) & 0x3f)) as u8);
        out.push((0x80 | ((scalar >> 6) & 0x3f)) as u8);
        out.push((0x80 | (scalar & 0x3f)) as u8);
    }
}

pub(crate) fn validate_header_size(
    offset: u64,
    header_size: u64,
    header_size_vint_len: usize,
) -> RarResult<()> {
    if header_size == 0 {
        return Err(RarError::CorruptArchive {
            detail: format!("zero header size at offset {offset}"),
        });
    }

    if header_size_vint_len > MAX_HEADER_SIZE_VINT_BYTES {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "header size vint at offset {} uses {} bytes, exceeding UnRAR RAR5 maximum {}",
                offset, header_size_vint_len, MAX_HEADER_SIZE_VINT_BYTES
            ),
        });
    }

    if header_size > MAX_HEADER_BODY {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "header size {} exceeds maximum {}",
                header_size, MAX_HEADER_BODY
            ),
        });
    }

    Ok(())
}

/// Build a RawHeader from pre-validated parts (used by encrypted header parser).
pub(crate) fn parse_raw_header_from_parts(
    offset: u64,
    crc32: u32,
    header_size: u64,
    header_size_vint_len: usize,
    body: Vec<u8>,
) -> crate::error::RarResult<RawHeader> {
    use crate::vint;

    validate_header_size(offset, header_size, header_size_vint_len)?;

    let mut pos = 0;
    let (header_type_val, n) = vint::read_vint(&body[pos..])?;
    pos += n;

    let (header_flags, n) = vint::read_vint(&body[pos..])?;
    pos += n;

    let extra_area_size = if header_flags & flags::EXTRA_AREA != 0 {
        let (size, n) = vint::read_vint(&body[pos..])?;
        pos += n;
        let _ = pos;
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

    Ok(RawHeader {
        offset,
        crc32,
        header_size,
        header_size_vint_len,
        header_type: HeaderType::from(header_type_val),
        flags: header_flags,
        extra_area_size,
        data_area_size,
        body,
    })
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
    let Ok(ea_size) = usize::try_from(raw.extra_area_size) else {
        return None;
    };
    let body_len = raw.body.len();
    if ea_size > body_len {
        return None;
    }
    Some(&raw.body[body_len - ea_size..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_prefix_decoder_accepts_overlong_sequences_like_unrar() {
        assert_eq!(decode_utf8_prefix_until_nul(b"a\xc0\xafpath"), "a/path");
    }

    #[test]
    fn utf8_prefix_decoder_keeps_prefix_before_malformed_continuation_like_unrar() {
        assert_eq!(decode_utf8_prefix_until_nul(b"valid/\xffignored"), "valid/");
        assert_eq!(decode_utf8_prefix_until_nul(b"valid/\xe2bad"), "valid/");
    }

    #[test]
    fn utf8_prefix_decoder_stops_at_nul_like_unrar() {
        assert_eq!(decode_utf8_prefix_until_nul(b"abc\0ignored"), "abc");
    }

    #[test]
    fn utf8_prefix_decoder_skips_out_of_range_four_byte_scalars_like_unrar() {
        assert_eq!(decode_utf8_prefix_until_nul(b"a\xf7\xbf\xbf\xbfb"), "ab");
    }

    #[test]
    fn utf8_prefix_decoder_preserves_surrogate_position_like_unrar() {
        assert_eq!(
            decode_utf8_prefix_until_nul(b"a\xed\xa0\x80b"),
            "a\u{fffd}b"
        );
    }

    #[test]
    fn utf8_prefix_decoder_exposes_unrar_wide_to_char_bytes() {
        let (text, bytes) = decode_utf8_prefix_until_nul_with_unrar_bytes(b"a\xc0\xaf\xed\xa0\x80");
        assert_eq!(text, "a/\u{fffd}");
        assert_eq!(bytes, b"a/\xed\xa0\x80");
    }

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
    fn test_crc_mismatch_warns_but_parses_like_unrar() {
        let mut data = build_header(1, 0, &[]);
        // Corrupt the stored CRC while leaving the body parseable. UnRAR reports
        // this damage but still attempts to process plaintext headers.
        data[0] ^= 0xFF;
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        assert_eq!(raw.header_type, HeaderType::MainArchive);
        assert_eq!(raw.flags, 0);
    }

    #[test]
    fn test_eof_returns_none() {
        let data: &[u8] = &[];
        let mut cursor = std::io::Cursor::new(data);
        let result = read_raw_header(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn max_header_body_matches_unrar_rar5_limit() {
        assert_eq!(MAX_HEADER_BODY, 0x200000);
    }

    #[test]
    fn header_size_vint_longer_than_three_bytes_is_rejected_like_unrar() {
        let mut data = Vec::new();
        data.extend_from_slice(&0u32.to_le_bytes());
        // Non-minimal four-byte vint encoding of value 1. UnRAR rejects RAR5
        // header-size vints that occupy more than three bytes regardless of
        // decoded value.
        data.extend_from_slice(&[0x81, 0x80, 0x80, 0x00]);

        let mut cursor = std::io::Cursor::new(data);
        let result = read_raw_header(&mut cursor);
        assert!(
            matches!(result, Err(RarError::CorruptArchive { ref detail }) if detail.contains("header size vint")),
            "expected CorruptArchive error for long header-size vint, got: {result:?}"
        );
    }

    #[test]
    fn pre_parsed_header_size_vint_longer_than_three_bytes_is_rejected_like_unrar() {
        let result = parse_raw_header_from_parts(0, 0, 1, 4, vec![1]);
        assert!(
            matches!(result, Err(RarError::CorruptArchive { ref detail }) if detail.contains("header size vint")),
            "expected CorruptArchive error for long pre-parsed header-size vint, got: {result:?}"
        );
    }

    #[test]
    fn pre_parsed_header_size_above_unrar_limit_is_rejected() {
        let result = parse_raw_header_from_parts(0, 0, MAX_HEADER_BODY + 1, 3, vec![1]);
        assert!(
            matches!(result, Err(RarError::CorruptArchive { ref detail }) if detail.contains("exceeds maximum")),
            "expected CorruptArchive error for oversized pre-parsed header, got: {result:?}"
        );
    }

    #[test]
    fn test_huge_header_rejected() {
        let huge_size: u64 = 17_000_000;
        let header_size_bytes = crate::vint::encode_vint(huge_size);

        // CRC doesn't matter since we'll error before checking it.
        let fake_crc: u32 = 0;
        let mut data = Vec::new();
        data.extend_from_slice(&fake_crc.to_le_bytes());
        data.extend_from_slice(&header_size_bytes);

        let mut cursor = std::io::Cursor::new(data);
        let result = read_raw_header(&mut cursor);
        assert!(
            matches!(result, Err(RarError::CorruptArchive { .. })),
            "expected CorruptArchive error for huge header, got: {result:?}"
        );
    }

    #[test]
    fn test_extra_area_bytes_huge_size_does_not_panic() {
        let raw = RawHeader {
            offset: 0,
            crc32: 0,
            header_size: 0,
            header_size_vint_len: 1,
            header_type: HeaderType::File,
            flags: flags::EXTRA_AREA,
            extra_area_size: u64::MAX,
            data_area_size: 0,
            body: vec![0; 8],
        };

        assert!(extra_area_bytes(&raw).is_none());
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
