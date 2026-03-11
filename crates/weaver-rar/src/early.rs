//! Early encryption detection from partial archive data.
//!
//! Designed for use during streaming download — works on a byte slice
//! without requiring the complete archive. The scheduler can call
//! [`detect_encryption`] on the first downloaded segment to determine
//! if a password is needed before downloading the remaining volumes.

use crate::signature::{RAR4_SIGNATURE, RAR5_SIGNATURE};
use crate::vint;

/// Result of early encryption detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionStatus {
    /// No encryption detected in the scanned headers.
    None,
    /// Archive headers are encrypted — cannot list files without password.
    HeaderEncrypted,
    /// File data is encrypted but headers are readable.
    FileEncrypted,
    /// Not enough data to determine encryption status.
    /// `min_bytes` is a conservative estimate of how many bytes are needed.
    Insufficient { min_bytes: usize },
    /// Data doesn't start with a RAR signature.
    NotRar,
}

/// Detect encryption from the first bytes of a RAR archive.
///
/// Works on a raw byte slice — no seeking or complete archive required.
/// Examines just the signature and first few headers to determine:
///
/// - **RAR5 header encryption**: encryption header (type 4) immediately after signature
/// - **RAR5 file encryption**: file header extra records with encryption type
/// - **RAR4 header encryption**: archive header `ENCRYPTED_HEADERS` flag (0x0080)
/// - **RAR4 file encryption**: file header `ENCRYPTED` flag (0x0004)
///
/// Returns [`EncryptionStatus::Insufficient`] if the buffer is too small to
/// make a determination — the caller should retry with more data.
pub fn detect_encryption(data: &[u8]) -> EncryptionStatus {
    // Try RAR5 first (8-byte signature), then RAR4 (7-byte).
    if data.len() >= 8 && data[..8] == RAR5_SIGNATURE {
        detect_rar5_encryption(&data[8..])
    } else if data.len() >= 7 && data[..7] == RAR4_SIGNATURE {
        detect_rar4_encryption(&data[7..])
    } else if data.len() < 8 {
        EncryptionStatus::Insufficient { min_bytes: 8 }
    } else {
        EncryptionStatus::NotRar
    }
}

/// Detect encryption in RAR5 data (positioned after the 8-byte signature).
fn detect_rar5_encryption(data: &[u8]) -> EncryptionStatus {
    // Parse the first header to check if it's an encryption header (type 4).
    let (header_type, header_end) = match parse_rar5_header_envelope(data) {
        Ok(v) => v,
        Err(need) => {
            return EncryptionStatus::Insufficient {
                min_bytes: 8 + need,
            };
        }
    };

    // RAR5 header type 4 = Encryption — all subsequent headers are encrypted.
    if header_type == 4 {
        return EncryptionStatus::HeaderEncrypted;
    }

    // First header should be MainArchive (type 1). Skip to next header.
    if header_type != 1 {
        // Unexpected header order — can't determine, report what we found.
        return EncryptionStatus::None;
    }

    // Try to parse the second header (should be a file or service header).
    let remaining = &data[header_end..];
    let (header_type2, header2_body, _header2_end) = match parse_rar5_header_with_body(remaining) {
        Ok(v) => v,
        Err(need) => {
            return EncryptionStatus::Insufficient {
                min_bytes: 8 + header_end + need,
            };
        }
    };

    // Type 2 = File, Type 3 = Service — both can have encryption extra records.
    if (header_type2 == 2 || header_type2 == 3) && has_rar5_encryption_extra(&header2_body) {
        return EncryptionStatus::FileEncrypted;
    }

    EncryptionStatus::None
}

/// Parse a RAR5 header envelope, returning (header_type, total_bytes_consumed).
/// On insufficient data, returns Err(min_bytes_needed_from_start_of_this_header).
fn parse_rar5_header_envelope(data: &[u8]) -> Result<(u64, usize), usize> {
    let mut pos = 0;

    // CRC32 (4 bytes)
    if data.len() < 4 {
        return Err(32);
    }
    pos += 4;

    // header_size vint
    let (header_size, n) = vint::read_vint(&data[pos..]).map_err(|_| pos + 16)?;
    pos += n;

    let body_start = pos;
    let total = body_start + header_size as usize;

    // Read header type from body
    let (header_type, _n) = vint::read_vint(&data[pos..]).map_err(|_| total)?;

    Ok((header_type, total))
}

/// Parse a RAR5 header, returning (header_type, body_bytes, total_bytes_consumed).
fn parse_rar5_header_with_body(data: &[u8]) -> Result<(u64, Vec<u8>, usize), usize> {
    let mut pos = 0;

    // CRC32 (4 bytes)
    if data.len() < 4 {
        return Err(32);
    }
    pos += 4;

    // header_size vint
    let (header_size, n) = vint::read_vint(&data[pos..]).map_err(|_| pos + 16)?;
    pos += n;

    let body_start = pos;
    let total = body_start + header_size as usize;

    if data.len() < total {
        return Err(total);
    }

    let body = data[body_start..total].to_vec();

    // Parse header type from body
    let (header_type, _) = vint::read_vint(&body).map_err(|_| total)?;

    Ok((header_type, body, total))
}

/// Check if a RAR5 file/service header body contains an encryption extra record.
///
/// Extra record layout: each record has size (vint), type (vint), then data.
/// Encryption extra record type = 1 (for file headers).
fn has_rar5_encryption_extra(body: &[u8]) -> bool {
    // Parse past common fields to find the extra area.
    let mut pos = 0;

    // header_type vint
    let Ok((_, n)) = vint::read_vint(&body[pos..]) else {
        return false;
    };
    pos += n;

    // header_flags vint
    let Ok((flags, n)) = vint::read_vint(&body[pos..]) else {
        return false;
    };
    pos += n;

    let has_extra = flags & 0x0001 != 0; // EXTRA_AREA flag
    if !has_extra {
        return false;
    }

    // extra_area_size vint
    let Ok((extra_area_size, _)) = vint::read_vint(&body[pos..]) else {
        return false;
    };

    // Extra area is the last `extra_area_size` bytes of the body.
    let ea_size = extra_area_size as usize;
    if ea_size > body.len() {
        return false;
    }
    let extra_area = &body[body.len() - ea_size..];

    // Walk extra records looking for encryption (type 1).
    let mut ea_pos = 0;
    while ea_pos < extra_area.len() {
        let Ok((record_size, n)) = vint::read_vint(&extra_area[ea_pos..]) else {
            break;
        };
        ea_pos += n;

        let record_end = ea_pos + record_size as usize;
        if record_end > extra_area.len() {
            break;
        }

        let Ok((record_type, _)) = vint::read_vint(&extra_area[ea_pos..]) else {
            break;
        };

        // File encryption extra record type = 1.
        if record_type == 1 {
            return true;
        }

        ea_pos = record_end;
    }

    false
}

/// Detect encryption in RAR4 data (positioned after the 7-byte signature).
fn detect_rar4_encryption(data: &[u8]) -> EncryptionStatus {
    // Archive header: CRC16(2) + type(1) + flags(2) + size(2) + reserved(6) = 13 bytes minimum
    if data.len() < 13 {
        return EncryptionStatus::Insufficient { min_bytes: 7 + 13 };
    }

    let header_type = data[2];
    if header_type != 0x73 {
        // Not an archive header — unexpected.
        return EncryptionStatus::None;
    }

    let flags = u16::from_le_bytes([data[3], data[4]]);
    let header_size = u16::from_le_bytes([data[5], data[6]]) as usize;

    // ENCRYPTED_HEADERS flag (0x0080)
    if flags & 0x0080 != 0 {
        return EncryptionStatus::HeaderEncrypted;
    }

    // Skip to the first file header.
    if data.len() < header_size {
        return EncryptionStatus::Insufficient {
            min_bytes: 7 + header_size + 7,
        };
    }
    let file_data = &data[header_size..];

    // File header: CRC16(2) + type(1) + flags(2) + size(2) = 7 bytes minimum
    if file_data.len() < 7 {
        return EncryptionStatus::Insufficient {
            min_bytes: 7 + header_size + 7,
        };
    }

    let file_type = file_data[2];
    if file_type != 0x74 {
        // Not a file header — might be a comment or other header. Can't determine.
        return EncryptionStatus::None;
    }

    let file_flags = u16::from_le_bytes([file_data[3], file_data[4]]);

    // ENCRYPTED flag (0x0004)
    if file_flags & 0x0004 != 0 {
        return EncryptionStatus::FileEncrypted;
    }

    EncryptionStatus::None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- RAR5 tests ----

    /// Build a minimal RAR5 header from type and body bytes.
    fn build_rar5_header(header_type: u64, header_flags: u64, type_body: &[u8]) -> Vec<u8> {
        // Body = header_type vint + header_flags vint + type_body
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(header_flags));
        body.extend_from_slice(type_body);

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

    fn encode_vint(mut value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            result.push(byte);
            if value == 0 {
                break;
            }
        }
        result
    }

    /// Build a RAR5 file header with an encryption extra record.
    fn build_rar5_encrypted_file_header() -> Vec<u8> {
        // Extra record: size(vint) + type(vint=1) + encryption data
        let mut extra_record = Vec::new();
        let record_type = encode_vint(1); // encryption extra type
        let record_data = vec![0u8; 10]; // dummy encryption params
        let record_size = encode_vint((record_type.len() + record_data.len()) as u64);
        extra_record.extend_from_slice(&record_size);
        extra_record.extend_from_slice(&record_type);
        extra_record.extend_from_slice(&record_data);

        let extra_area_size = extra_record.len() as u64;

        // Type-specific file fields (minimal): file_flags(vint) + unpacked_size(vint)
        // + ... we need enough to fill body before the extra area.
        // The extra area is the LAST extra_area_size bytes of the body.
        let mut type_body = Vec::new();
        // file_flags
        type_body.extend_from_slice(&encode_vint(0));
        // unpacked_size
        type_body.extend_from_slice(&encode_vint(1000));
        // attributes
        type_body.extend_from_slice(&encode_vint(0));
        // mtime (4 bytes)
        type_body.extend_from_slice(&0u32.to_le_bytes());
        // data_crc (4 bytes)
        type_body.extend_from_slice(&0u32.to_le_bytes());
        // compression_info (vint)
        type_body.extend_from_slice(&encode_vint(0));
        // host_os (vint)
        type_body.extend_from_slice(&encode_vint(0));
        // name_length (vint) + name
        let name = b"test.dat";
        type_body.extend_from_slice(&encode_vint(name.len() as u64));
        type_body.extend_from_slice(name);
        // Extra area bytes appended at the end
        type_body.extend_from_slice(&extra_record);

        // header_flags: EXTRA_AREA (0x0001)
        let header_flags = 0x0001u64;

        // Build body: header_type + header_flags + extra_area_size + type_body
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(2)); // File header type
        body.extend_from_slice(&encode_vint(header_flags));
        body.extend_from_slice(&encode_vint(extra_area_size));
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
    fn rar5_header_encrypted() {
        let mut data = RAR5_SIGNATURE.to_vec();
        // Encryption header (type 4)
        let enc_header = build_rar5_header(4, 0, &[0; 20]);
        data.extend_from_slice(&enc_header);

        assert_eq!(detect_encryption(&data), EncryptionStatus::HeaderEncrypted);
    }

    #[test]
    fn rar5_not_encrypted() {
        let mut data = RAR5_SIGNATURE.to_vec();
        // Main archive header (type 1), then a file header with no encryption
        let main_header = build_rar5_header(1, 0, &encode_vint(0)); // archive_flags = 0
        let file_header = build_rar5_header(2, 0, &[]); // no extra area
        data.extend_from_slice(&main_header);
        data.extend_from_slice(&file_header);

        assert_eq!(detect_encryption(&data), EncryptionStatus::None);
    }

    #[test]
    fn rar5_file_encrypted() {
        let mut data = RAR5_SIGNATURE.to_vec();
        let main_header = build_rar5_header(1, 0, &encode_vint(0));
        let file_header = build_rar5_encrypted_file_header();
        data.extend_from_slice(&main_header);
        data.extend_from_slice(&file_header);

        assert_eq!(detect_encryption(&data), EncryptionStatus::FileEncrypted);
    }

    #[test]
    fn rar5_insufficient_data() {
        // Just the signature, no headers
        let data = RAR5_SIGNATURE.to_vec();
        assert!(matches!(
            detect_encryption(&data),
            EncryptionStatus::Insufficient { .. }
        ));
    }

    #[test]
    fn rar5_insufficient_for_file_header() {
        let mut data = RAR5_SIGNATURE.to_vec();
        let main_header = build_rar5_header(1, 0, &encode_vint(0));
        data.extend_from_slice(&main_header);
        // No file header data follows

        assert!(matches!(
            detect_encryption(&data),
            EncryptionStatus::Insufficient { .. }
        ));
    }

    // ---- RAR4 tests ----

    fn build_rar4_archive_header(flags: u16) -> Vec<u8> {
        let header_size: u16 = 7 + 6; // common 7 + reserved 6
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x73); // type
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&header_size.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]); // reserved
        buf
    }

    fn build_rar4_file_header(flags: u16) -> Vec<u8> {
        let name = b"test.dat";
        let header_size: u16 = 7 + 25 + name.len() as u16;
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x74); // type = file
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&header_size.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // packed size
        buf.extend_from_slice(&100u32.to_le_bytes()); // unpacked size
        buf.push(3); // host OS = Unix
        buf.extend_from_slice(&0u32.to_le_bytes()); // CRC32
        buf.extend_from_slice(&0u32.to_le_bytes()); // datetime
        buf.push(29); // version
        buf.push(0x30); // method = Store
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // attrs
        buf.extend_from_slice(name);
        buf
    }

    #[test]
    fn rar4_header_encrypted() {
        let mut data = RAR4_SIGNATURE.to_vec();
        data.extend_from_slice(&build_rar4_archive_header(0x0080)); // ENCRYPTED_HEADERS
        assert_eq!(detect_encryption(&data), EncryptionStatus::HeaderEncrypted);
    }

    #[test]
    fn rar4_file_encrypted() {
        let mut data = RAR4_SIGNATURE.to_vec();
        data.extend_from_slice(&build_rar4_archive_header(0));
        data.extend_from_slice(&build_rar4_file_header(0x0004)); // ENCRYPTED
        assert_eq!(detect_encryption(&data), EncryptionStatus::FileEncrypted);
    }

    #[test]
    fn rar4_not_encrypted() {
        let mut data = RAR4_SIGNATURE.to_vec();
        data.extend_from_slice(&build_rar4_archive_header(0));
        data.extend_from_slice(&build_rar4_file_header(0));
        assert_eq!(detect_encryption(&data), EncryptionStatus::None);
    }

    #[test]
    fn rar4_insufficient_data() {
        let data = RAR4_SIGNATURE.to_vec(); // no archive header
        assert!(matches!(
            detect_encryption(&data),
            EncryptionStatus::Insufficient { .. }
        ));
    }

    // ---- General tests ----

    #[test]
    fn not_rar() {
        let data = b"PK\x03\x04this is a zip file";
        assert_eq!(detect_encryption(data), EncryptionStatus::NotRar);
    }

    #[test]
    fn too_short_for_signature() {
        let data = b"Rar";
        assert!(matches!(
            detect_encryption(data),
            EncryptionStatus::Insufficient { .. }
        ));
    }

    #[test]
    fn empty_data() {
        assert!(matches!(
            detect_encryption(&[]),
            EncryptionStatus::Insufficient { .. }
        ));
    }
}
