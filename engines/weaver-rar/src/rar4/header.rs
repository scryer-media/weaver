//! RAR4 header parsing.
//!
//! RAR4 headers use fixed-size fields (u16/u32 LE). Each header starts with:
//! - CRC16 (2 bytes): CRC of header data starting from header_type
//! - Header type (1 byte)
//! - Flags (2 bytes)
//! - Header size (2 bytes): total header size including these 7 bytes
//! - Optional: data size (4 bytes) if HAS_DATA flag set
//!
//! Reference: RAR 4.x technical note, libarchive (BSD).

use std::io::{Read, Seek, SeekFrom};

use super::types::*;
use crate::error::{RarError, RarResult};

/// Minimum header size: CRC(2) + type(1) + flags(2) + size(2) = 7 bytes.
const MIN_HEADER_SIZE: usize = 7;

/// Maximum header size to prevent abuse.
const MAX_HEADER_SIZE: usize = 2 * 1024 * 1024; // 2 MB

/// Read a u16 LE from a byte slice at the given offset.
fn read_u16(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

/// Read a u32 LE from a byte slice at the given offset.
fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

/// Raw header data as read from the stream.
#[derive(Debug)]
pub struct RawRar4Header {
    pub header_type: Rar4HeaderType,
    pub flags: u16,
    pub header_size: u16,
    pub data: Vec<u8>,
    /// Offset in the stream where this header started.
    pub offset: u64,
    /// Size of the data area following the header (0 if none).
    pub data_area_size: u64,
}

/// Read one raw RAR4 header from the stream.
///
/// Returns None on EOF.
pub fn read_raw_header<R: Read + Seek>(reader: &mut R) -> RarResult<Option<RawRar4Header>> {
    let offset = reader.stream_position().map_err(RarError::Io)?;

    // Read minimum header bytes.
    let mut buf = [0u8; MIN_HEADER_SIZE];
    match reader.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RarError::Io(e)),
    }

    let _crc16 = read_u16(&buf, 0);
    let header_type = Rar4HeaderType::from(buf[2]);
    let flags = read_u16(&buf, 3);
    let header_size = read_u16(&buf, 5);

    if (header_size as usize) < MIN_HEADER_SIZE {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR4 header size {} is less than minimum {MIN_HEADER_SIZE}",
                header_size
            ),
        });
    }

    if header_size as usize > MAX_HEADER_SIZE {
        return Err(RarError::ResourceLimit {
            detail: format!("RAR4 header size {} exceeds maximum", header_size),
        });
    }

    // Read the full header.
    let mut data = vec![0u8; header_size as usize];
    data[..MIN_HEADER_SIZE].copy_from_slice(&buf);
    if header_size as usize > MIN_HEADER_SIZE {
        reader
            .read_exact(&mut data[MIN_HEADER_SIZE..])
            .map_err(RarError::Io)?;
    }

    // Determine data area size.
    let data_area_size = if flags & common_flags::HAS_DATA != 0 {
        if data.len() >= 11 {
            read_u32(&data, 7) as u64
        } else {
            0
        }
    } else {
        0
    };

    Ok(Some(RawRar4Header {
        header_type,
        flags,
        header_size,
        data,
        offset,
        data_area_size,
    }))
}

/// Parse a RAR4 archive header from raw header data.
pub fn parse_archive_header(raw: &RawRar4Header) -> RarResult<Rar4ArchiveHeader> {
    Ok(Rar4ArchiveHeader {
        flags: raw.flags,
        is_volume: raw.flags & archive_flags::VOLUME != 0,
        is_solid: raw.flags & archive_flags::SOLID != 0,
        is_encrypted: raw.flags & archive_flags::ENCRYPTED_HEADERS != 0,
        is_first_volume: raw.flags & archive_flags::FIRST_VOLUME != 0,
        new_naming: raw.flags & archive_flags::NEW_NUMBERING != 0,
    })
}

/// Parse a RAR4 file header from raw header data.
pub fn parse_file_header(raw: &RawRar4Header) -> RarResult<Rar4FileHeader> {
    let data = &raw.data;
    if data.len() < 32 {
        return Err(RarError::CorruptArchive {
            detail: format!("RAR4 file header too short: {} bytes", data.len()),
        });
    }

    // Fixed fields after the common 7-byte header:
    // Offset 7: packed size (4 bytes, low 32 bits)
    // Offset 11: unpacked size (4 bytes, low 32 bits)
    // Offset 15: host OS (1 byte)
    // Offset 16: CRC32 (4 bytes)
    // Offset 20: date/time (4 bytes, DOS format)
    // Offset 24: unpack version (1 byte)
    // Offset 25: method (1 byte)
    // Offset 26: name length (2 bytes)
    // Offset 28: attributes (4 bytes)
    let packed_low = read_u32(data, 7) as u64;
    let unpacked_low = read_u32(data, 11) as u64;
    let host_os = Rar4HostOs::from(data[15]);
    let crc32 = read_u32(data, 16);
    let mtime = read_u32(data, 20);
    let _unpack_version = data[24];
    let method = Rar4Method::from(data[25]);
    let name_len = read_u16(data, 26) as usize;
    let attributes = read_u32(data, 28);

    // High 32 bits of sizes (if LARGE flag set).
    let mut packed_size = packed_low;
    let mut unpacked_size = unpacked_low;
    let mut pos = 32;

    if raw.flags & file_flags::LARGE != 0 {
        if data.len() < pos + 8 {
            return Err(RarError::CorruptArchive {
                detail: "RAR4 file header LARGE flag set but insufficient data".into(),
            });
        }
        let packed_high = read_u32(data, pos) as u64;
        let unpacked_high = read_u32(data, pos + 4) as u64;
        packed_size |= packed_high << 32;
        unpacked_size |= unpacked_high << 32;
        pos += 8;
    }

    // Read filename.
    if data.len() < pos + name_len {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR4 file header name extends past header: need {} bytes at offset {}, have {}",
                name_len,
                pos,
                data.len()
            ),
        });
    }
    let name_bytes = &data[pos..pos + name_len];
    pos += name_len;

    // Decode filename. If UNICODE flag, the name is encoded with RAR4's unicode
    // scheme. Otherwise, it's an OEM/ASCII name.
    let name = if raw.flags & file_flags::UNICODE != 0 {
        decode_rar4_unicode_name(name_bytes)
    } else {
        String::from_utf8_lossy(name_bytes).into_owned()
    };

    // Read salt if encrypted.
    let salt = if raw.flags & file_flags::SALT != 0 {
        if data.len() < pos + 8 {
            return Err(RarError::CorruptArchive {
                detail: "RAR4 file header SALT flag set but insufficient data".into(),
            });
        }
        let mut s = [0u8; 8];
        s.copy_from_slice(&data[pos..pos + 8]);
        Some(s)
    } else {
        None
    };

    let is_directory = attributes & 0x10 != 0 || (raw.flags & 0xE0) == 0xE0; // directory attribute in flags

    let data_offset = raw.offset + raw.header_size as u64;

    Ok(Rar4FileHeader {
        flags: raw.flags,
        packed_size,
        unpacked_size,
        host_os,
        crc32,
        mtime,
        method,
        name,
        is_directory,
        is_encrypted: raw.flags & file_flags::ENCRYPTED != 0,
        is_solid: raw.flags & file_flags::SOLID != 0,
        split_before: raw.flags & file_flags::SPLIT_BEFORE != 0,
        split_after: raw.flags & file_flags::SPLIT_AFTER != 0,
        data_offset,
        salt,
        attributes,
    })
}

/// Parse a RAR4 end-of-archive header.
///
/// The ENDARC header may contain optional data after the 7-byte common header:
/// - If DATA_CRC flag (0x0002): 4 bytes archive data CRC
/// - If VOLUME_NUMBER flag (0x0004): 2 bytes volume number (0-based)
pub fn parse_end_header(raw: &RawRar4Header) -> Rar4EndHeader {
    let data = &raw.data;
    let mut pos = 7; // skip common header

    // Skip data CRC if present.
    if raw.flags & end_flags::DATA_CRC != 0 {
        pos += 4;
    }

    // Read volume number if present.
    let volume_number = if raw.flags & end_flags::VOLUME_NUMBER != 0 && data.len() >= pos + 2 {
        Some(read_u16(data, pos))
    } else {
        None
    };

    Rar4EndHeader {
        flags: raw.flags,
        more_volumes: raw.flags & end_flags::NEXT_VOLUME != 0,
        volume_number,
    }
}

/// Decode a RAR4 unicode filename.
///
/// RAR4 unicode encoding: the buffer contains the ASCII name followed by a
/// null byte, then a compact unicode encoding. If there's no null byte,
/// the whole thing is just ASCII/OEM.
fn decode_rar4_unicode_name(data: &[u8]) -> String {
    // Find the null separator between ASCII and unicode portions.
    let null_pos = data.iter().position(|&b| b == 0);

    let Some(null_pos) = null_pos else {
        // No null found — treat as plain ASCII.
        return String::from_utf8_lossy(data).into_owned();
    };

    let ascii_name = &data[..null_pos];
    let unicode_data = &data[null_pos + 1..];

    if unicode_data.is_empty() {
        return String::from_utf8_lossy(ascii_name).into_owned();
    }

    // Decode the unicode portion. RAR4 uses a compact encoding:
    // Bytes are processed in pairs (flags byte + data bytes).
    // Each flags byte controls 4 character slots (2 bits each):
    //   00 = use ASCII byte directly
    //   01 = use ASCII byte + high byte from current state
    //   10 = use 2-byte unicode char
    //   11 = use ASCII byte + increment position
    let mut result = Vec::with_capacity(ascii_name.len() * 2);
    let mut ui = 0; // unicode data index
    let mut ai = 0; // ascii index
    let mut high_byte = 0u8;

    while ui < unicode_data.len() {
        let flags = unicode_data[ui];
        ui += 1;

        for shift in (0..8).step_by(2) {
            if ai >= ascii_name.len() {
                break;
            }

            let mode = (flags >> (6 - shift)) & 0x03;
            match mode {
                0 => {
                    // Direct ASCII byte.
                    result.push(ascii_name[ai] as u16);
                    ai += 1;
                }
                1 => {
                    // ASCII byte + high byte.
                    if ui >= unicode_data.len() {
                        break;
                    }
                    result.push(unicode_data[ui] as u16 | (high_byte as u16) << 8);
                    ui += 1;
                    ai += 1;
                }
                2 => {
                    // Two-byte unicode character.
                    if ui + 1 >= unicode_data.len() {
                        break;
                    }
                    result.push(unicode_data[ui] as u16 | (unicode_data[ui + 1] as u16) << 8);
                    ui += 2;
                    ai += 1;
                }
                3 => {
                    // Set high byte and copy ASCII.
                    if ui >= unicode_data.len() {
                        break;
                    }
                    let count = unicode_data[ui] as usize;
                    ui += 1;
                    if ui >= unicode_data.len() {
                        break;
                    }
                    high_byte = unicode_data[ui];
                    ui += 1;
                    for _ in 0..count {
                        if ai >= ascii_name.len() {
                            break;
                        }
                        result.push(ascii_name[ai] as u16 | (high_byte as u16) << 8);
                        ai += 1;
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    // Append any remaining ASCII characters.
    while ai < ascii_name.len() {
        result.push(ascii_name[ai] as u16);
        ai += 1;
    }

    String::from_utf16_lossy(&result)
}

/// Read one raw RAR4 header from an encrypted stream.
///
/// RAR4 header-level encryption (`-hp`) encrypts all headers after the archive
/// header as a continuous AES-128-CBC stream. This function reads aligned
/// 16-byte blocks from the stream, decrypts them through the stateful CBC
/// decryptor, and parses the result as a normal header.
///
/// The decryptor's IV state persists across calls — file data areas between
/// headers are seeked past in the raw stream without feeding through the
/// decryptor.
pub fn read_raw_header_encrypted<R: Read>(
    reader: &mut R,
    decryptor: &mut crate::crypto::Rar4CbcDecryptor,
) -> RarResult<Option<RawRar4Header>> {
    // Read first AES block (16 bytes) — contains the 7-byte common header.
    let mut block = [0u8; 16];
    match reader.read_exact(&mut block) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RarError::Io(e)),
    }
    decryptor.decrypt_blocks(&mut block);

    // Parse the 7-byte common header from decrypted data.
    let header_type = Rar4HeaderType::from(block[2]);
    let flags = read_u16(&block, 3);
    let header_size = read_u16(&block, 5);

    if (header_size as usize) < MIN_HEADER_SIZE {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR4 encrypted header size {} < minimum {MIN_HEADER_SIZE}",
                header_size
            ),
        });
    }

    if header_size as usize > MAX_HEADER_SIZE {
        return Err(RarError::ResourceLimit {
            detail: format!("RAR4 encrypted header size {} exceeds maximum", header_size),
        });
    }

    // Total encrypted bytes = header_size rounded up to 16-byte boundary.
    let aligned_size = ((header_size as usize) + 15) & !15;

    // We already decrypted 16 bytes. Read and decrypt any remaining blocks.
    let mut decrypted = Vec::with_capacity(aligned_size);
    decrypted.extend_from_slice(&block);

    if aligned_size > 16 {
        let remaining = aligned_size - 16;
        let mut more = vec![0u8; remaining];
        reader.read_exact(&mut more).map_err(RarError::Io)?;
        decryptor.decrypt_blocks(&mut more);
        decrypted.extend_from_slice(&more);
    }

    // Extract the actual header bytes (header_size bytes, ignoring padding).
    let data = decrypted[..header_size as usize].to_vec();

    // Determine data area size (same logic as plaintext).
    let data_area_size = if flags & common_flags::HAS_DATA != 0 {
        if data.len() >= 11 {
            read_u32(&data, 7) as u64
        } else {
            0
        }
    } else {
        0
    };

    Ok(Some(RawRar4Header {
        header_type,
        flags,
        header_size,
        data,
        offset: 0, // Not meaningful for encrypted headers.
        data_area_size,
    }))
}

/// Skip the data area of a RAR4 header.
pub fn skip_data_area<R: Read + Seek>(reader: &mut R, raw: &RawRar4Header) -> RarResult<()> {
    if raw.data_area_size > 0 {
        reader
            .seek(SeekFrom::Current(raw.data_area_size as i64))
            .map_err(RarError::Io)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal RAR4 header in a buffer.
    fn build_raw_header(header_type: u8, flags: u16, extra_data: &[u8]) -> Vec<u8> {
        let header_size = (MIN_HEADER_SIZE + extra_data.len()) as u16;
        let mut buf = Vec::new();
        // CRC16 placeholder (2 bytes)
        buf.extend_from_slice(&[0x00, 0x00]);
        // Header type
        buf.push(header_type);
        // Flags
        buf.extend_from_slice(&flags.to_le_bytes());
        // Header size
        buf.extend_from_slice(&header_size.to_le_bytes());
        // Extra data
        buf.extend_from_slice(extra_data);
        buf
    }

    #[test]
    fn test_read_raw_archive_header() {
        let data = build_raw_header(0x73, 0x0000, &[0x00; 6]);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        assert_eq!(raw.header_type, Rar4HeaderType::Archive);
        assert_eq!(raw.flags, 0);
    }

    #[test]
    fn test_read_raw_header_eof() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_raw_header(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_archive_header_flags() {
        let data = build_raw_header(0x73, archive_flags::SOLID | archive_flags::VOLUME, &[0; 6]);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let arch = parse_archive_header(&raw).unwrap();
        assert!(arch.is_solid);
        assert!(arch.is_volume);
        assert!(!arch.is_encrypted);
    }

    #[test]
    fn test_parse_file_header_minimal() {
        // Build a file header with minimum required data.
        // After common 7 bytes: packed(4) + unpacked(4) + os(1) + crc(4) +
        // datetime(4) + version(1) + method(1) + name_len(2) + attrs(4) = 25 bytes
        // Then name bytes.
        let name = b"test.txt";
        let mut extra = Vec::new();
        extra.extend_from_slice(&100u32.to_le_bytes()); // packed size
        extra.extend_from_slice(&200u32.to_le_bytes()); // unpacked size
        extra.push(3); // Unix
        extra.extend_from_slice(&0xDEADBEEFu32.to_le_bytes()); // CRC32
        extra.extend_from_slice(&0x00000000u32.to_le_bytes()); // datetime
        extra.push(29); // unpack version
        extra.push(0x33); // method: Normal
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes()); // name length
        extra.extend_from_slice(&0u32.to_le_bytes()); // attributes
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, 0x0000, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "test.txt");
        assert_eq!(fh.packed_size, 100);
        assert_eq!(fh.unpacked_size, 200);
        assert_eq!(fh.host_os, Rar4HostOs::Unix);
        assert_eq!(fh.crc32, 0xDEADBEEF);
        assert_eq!(fh.method, Rar4Method::Normal);
        assert!(!fh.is_encrypted);
        assert!(!fh.is_directory);
    }

    #[test]
    fn test_parse_file_header_large_sizes() {
        let name = b"big.dat";
        let mut extra = Vec::new();
        extra.extend_from_slice(&0xFFFFFFFFu32.to_le_bytes()); // packed low
        extra.extend_from_slice(&0xFFFFFFFFu32.to_le_bytes()); // unpacked low
        extra.push(2); // Windows
        extra.extend_from_slice(&0u32.to_le_bytes()); // CRC
        extra.extend_from_slice(&0u32.to_le_bytes()); // datetime
        extra.push(29);
        extra.push(0x30); // Store
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes()); // attrs
        // High 32 bits
        extra.extend_from_slice(&1u32.to_le_bytes()); // packed high
        extra.extend_from_slice(&2u32.to_le_bytes()); // unpacked high
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, file_flags::LARGE, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.packed_size, 0x1_FFFFFFFF);
        assert_eq!(fh.unpacked_size, 0x2_FFFFFFFF);
    }

    #[test]
    fn test_unicode_name_no_null() {
        let name = b"simple.txt";
        let result = decode_rar4_unicode_name(name);
        assert_eq!(result, "simple.txt");
    }

    #[test]
    fn test_unicode_name_with_null_no_unicode() {
        let mut data = b"name.txt".to_vec();
        data.push(0);
        let result = decode_rar4_unicode_name(&data);
        assert_eq!(result, "name.txt");
    }

    #[test]
    fn test_end_header() {
        let data = build_raw_header(0x7B, 0x0001, &[]);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let end = parse_end_header(&raw);
        assert!(end.more_volumes);
    }

    #[test]
    fn test_header_too_short() {
        // Header size claims only 3 bytes.
        let mut buf = vec![0x00, 0x00, 0x73]; // CRC + type
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags
        buf.extend_from_slice(&3u16.to_le_bytes()); // header_size = 3 (too small)
        let mut cursor = Cursor::new(buf);
        let result = read_raw_header(&mut cursor);
        assert!(result.is_err());
    }
}
