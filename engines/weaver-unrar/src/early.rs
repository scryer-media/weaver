//! Early encryption detection from partial archive data.
//!
//! Designed for use during streaming download — works on a byte slice
//! without requiring the complete archive. The scheduler can call
//! [`detect_encryption`] on the first downloaded segment to determine
//! if a password is needed before downloading the remaining volumes.

use crate::signature::{
    DEFAULT_SFX_MAX_SCAN, RAR4_SIGNATURE, RAR5_SIGNATURE, RAR14_SFX_MARKER, RAR14_SIGNATURE,
};
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
/// - **RAR14 file encryption**: file header `LHD_PASSWORD` flag (0x0004)
///
/// Returns [`EncryptionStatus::Insufficient`] if the buffer is too small to
/// make a determination — the caller should retry with more data.
pub fn detect_encryption(data: &[u8]) -> EncryptionStatus {
    if let Some(signature) = find_rar_signature(data) {
        let status = match signature.format {
            EarlyArchiveFormat::Rar5 => {
                detect_rar5_encryption(&data[signature.offset + RAR5_SIGNATURE.len()..])
            }
            EarlyArchiveFormat::Rar4 => {
                detect_rar4_encryption(&data[signature.offset + RAR4_SIGNATURE.len()..])
            }
            EarlyArchiveFormat::Rar14 => {
                detect_rar14_encryption(&data[signature.offset + RAR14_SIGNATURE.len()..])
            }
        };
        add_signature_offset(status, signature.offset)
    } else if data.len() < 8 {
        EncryptionStatus::Insufficient { min_bytes: 8 }
    } else {
        EncryptionStatus::NotRar
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EarlyArchiveFormat {
    Rar14,
    Rar4,
    Rar5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EarlySignature {
    format: EarlyArchiveFormat,
    offset: usize,
}

fn add_signature_offset(status: EncryptionStatus, offset: usize) -> EncryptionStatus {
    match status {
        EncryptionStatus::Insufficient { min_bytes } if offset != 0 => {
            EncryptionStatus::Insufficient {
                min_bytes: min_bytes.saturating_add(offset),
            }
        }
        other => other,
    }
}

fn find_rar_signature(data: &[u8]) -> Option<EarlySignature> {
    if data.len() >= RAR5_SIGNATURE.len() && data[..RAR5_SIGNATURE.len()] == RAR5_SIGNATURE {
        return Some(EarlySignature {
            format: EarlyArchiveFormat::Rar5,
            offset: 0,
        });
    }
    if data.len() >= RAR4_SIGNATURE.len() && data[..RAR4_SIGNATURE.len()] == RAR4_SIGNATURE {
        return Some(EarlySignature {
            format: EarlyArchiveFormat::Rar4,
            offset: 0,
        });
    }
    if data.len() >= RAR14_SIGNATURE.len() && data[..RAR14_SIGNATURE.len()] == RAR14_SIGNATURE {
        return Some(EarlySignature {
            format: EarlyArchiveFormat::Rar14,
            offset: 0,
        });
    }

    // After the initial 7-byte mark read fails, scan the bounded SFX window
    // while reserving the last 16 bytes.
    let scan_start = RAR4_SIGNATURE.len();
    let scan_len = DEFAULT_SFX_MAX_SCAN.saturating_sub(16) as usize;
    let scan_end = data.len().min(scan_start.saturating_add(scan_len));
    if scan_start >= scan_end {
        return None;
    }

    let mut found = None;
    for offset in scan_start..scan_end {
        let format = if data[offset..].starts_with(&RAR5_SIGNATURE) {
            EarlyArchiveFormat::Rar5
        } else if data[offset..].starts_with(&RAR4_SIGNATURE) {
            EarlyArchiveFormat::Rar4
        } else if data[offset..].starts_with(&RAR14_SIGNATURE)
            && rar14_sfx_candidate_allowed(data, offset, scan_start)
        {
            EarlyArchiveFormat::Rar14
        } else {
            continue;
        };
        found = Some(EarlySignature { format, offset });
        break;
    }
    found
}

fn rar14_sfx_candidate_allowed(data: &[u8], offset: usize, scan_start: usize) -> bool {
    if offset <= scan_start {
        return true;
    }

    let marker_start = 28usize;
    let marker_end = marker_start + RAR14_SFX_MARKER.len();
    if scan_start >= marker_start || data.len() < marker_end {
        return true;
    }
    data[marker_start..marker_end] == RAR14_SFX_MARKER
}

/// Detect encryption in RAR5 data (positioned after the 8-byte signature).
fn detect_rar5_encryption(data: &[u8]) -> EncryptionStatus {
    let mut offset = 0usize;

    loop {
        let header = match parse_rar5_header(data, offset) {
            Ok(header) => header,
            Err(need) => {
                return EncryptionStatus::Insufficient {
                    min_bytes: 8 + need,
                };
            }
        };

        if header.header_type == 4 {
            return EncryptionStatus::HeaderEncrypted;
        }

        if (header.header_type == 2 || header.header_type == 3)
            && has_rar5_encryption_extra(header.header_flags, &header.body)
        {
            return EncryptionStatus::FileEncrypted;
        }

        offset = header.next_header_offset;
        if header.header_type == 5 {
            return EncryptionStatus::None;
        }
    }
}

struct ParsedRar5Header {
    header_type: u64,
    header_flags: u64,
    body: Vec<u8>,
    next_header_offset: usize,
}

/// Parse a RAR5 header starting at `offset`.
fn parse_rar5_header(data: &[u8], offset: usize) -> Result<ParsedRar5Header, usize> {
    let remaining = &data[offset..];
    let mut pos = 0usize;

    if remaining.len() < 4 {
        return Err(offset + 32);
    }
    pos += 4;

    let (header_size, n) = vint::read_vint(&remaining[pos..]).map_err(|_| offset + pos + 16)?;
    pos += n;

    let body_start = pos;
    let total = body_start + header_size as usize;
    if remaining.len() < total {
        return Err(offset + total);
    }

    let body = remaining[body_start..total].to_vec();

    let (header_type, n) = vint::read_vint(&body).map_err(|_| offset + total)?;
    let (header_flags, mut body_pos) = vint::read_vint(&body[n..]).map_err(|_| offset + total)?;
    body_pos += n;

    if header_flags & 0x0001 != 0 {
        let (_, extra_n) = vint::read_vint(&body[body_pos..]).map_err(|_| offset + total)?;
        body_pos += extra_n;
    }
    let data_area_size = if header_flags & 0x0002 != 0 {
        let (value, _) = vint::read_vint(&body[body_pos..]).map_err(|_| offset + total)?;
        value as usize
    } else {
        0
    };

    let next_header_offset = offset + total + data_area_size;
    if data.len() < next_header_offset {
        return Err(next_header_offset);
    }

    Ok(ParsedRar5Header {
        header_type,
        header_flags,
        body,
        next_header_offset,
    })
}

/// Check if a RAR5 file/service header body contains an encryption extra record.
///
/// Extra record layout: each record has size (vint), type (vint), then data.
/// Encryption extra record type = 1 (for file headers).
fn has_rar5_encryption_extra(header_flags: u64, body: &[u8]) -> bool {
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

    if header_flags & 0x0001 == 0 {
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

/// Detect encryption in RAR14 data (positioned after the 4-byte signature).
fn detect_rar14_encryption(data: &[u8]) -> EncryptionStatus {
    // RAR14 main header after the 4-byte marker: HeadSize(2) + flags(1).
    if data.len() < 3 {
        return EncryptionStatus::Insufficient {
            min_bytes: RAR14_SIGNATURE.len() + 3,
        };
    }

    let main_header_size = u16::from_le_bytes([data[0], data[1]]) as usize;
    if main_header_size < 7 {
        return EncryptionStatus::None;
    }
    let main_body_after_signature = main_header_size.saturating_sub(RAR14_SIGNATURE.len());
    if data.len() < main_body_after_signature + 21 {
        return EncryptionStatus::Insufficient {
            min_bytes: main_header_size + 21,
        };
    }

    let file_data = &data[main_body_after_signature..];
    let file_header_size = u16::from_le_bytes([file_data[10], file_data[11]]) as usize;
    if file_header_size < 21 {
        return EncryptionStatus::None;
    }

    // LHD_PASSWORD flag.
    if file_data[17] & 0x04 != 0 {
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

    fn build_rar5_plain_file_header() -> Vec<u8> {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(0));
        type_body.extend_from_slice(&encode_vint(1000));
        type_body.extend_from_slice(&encode_vint(0));
        type_body.extend_from_slice(&0u32.to_le_bytes());
        type_body.extend_from_slice(&0u32.to_le_bytes());
        type_body.extend_from_slice(&encode_vint(0));
        type_body.extend_from_slice(&encode_vint(0));
        let name = b"test.dat";
        type_body.extend_from_slice(&encode_vint(name.len() as u64));
        type_body.extend_from_slice(name);

        build_rar5_header(2, 0, &type_body)
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
        let file_header = build_rar5_plain_file_header();
        data.extend_from_slice(&main_header);
        data.extend_from_slice(&file_header);

        assert!(matches!(
            detect_encryption(&data),
            EncryptionStatus::Insufficient { .. }
        ));
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
    fn sfx_rar5_file_encrypted() {
        let stub = b"MZ small self-extracting stub";
        let mut data = stub.to_vec();
        data.extend_from_slice(&RAR5_SIGNATURE);
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
    fn sfx_rar5_insufficient_data_adds_stub_offset() {
        let stub = b"MZ encrypted SFX stub";
        let mut data = stub.to_vec();
        data.extend_from_slice(&RAR5_SIGNATURE);

        match detect_encryption(&data) {
            EncryptionStatus::Insufficient { min_bytes } => {
                assert!(min_bytes > stub.len() + RAR5_SIGNATURE.len());
            }
            other => panic!("expected Insufficient, got {other:?}"),
        }
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

    fn build_rar14_archive_header(flags: u8) -> Vec<u8> {
        let mut buf = RAR14_SIGNATURE.to_vec();
        buf.extend_from_slice(&7u16.to_le_bytes());
        buf.push(flags);
        buf
    }

    fn build_rar14_file_header(flags: u8) -> Vec<u8> {
        let name = b"old.dat";
        let head_size = 21u16 + name.len() as u16;
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes()); // packed size
        buf.extend_from_slice(&0u32.to_le_bytes()); // unpacked size
        buf.extend_from_slice(&0u16.to_le_bytes()); // checksum
        buf.extend_from_slice(&head_size.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // DOS time
        buf.push(0x20); // attributes
        buf.push(flags);
        buf.push(2); // RAR 1.3-compatible unpack version
        buf.push(name.len() as u8);
        buf.push(0x30); // method = Store
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
    fn sfx_rar4_header_encrypted() {
        let mut data = b"MZ legacy self-extracting stub".to_vec();
        data.extend_from_slice(&RAR4_SIGNATURE);
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

    #[test]
    fn rar14_file_encrypted() {
        let mut data = build_rar14_archive_header(0);
        data.extend_from_slice(&build_rar14_file_header(0x04)); // LHD_PASSWORD
        assert_eq!(detect_encryption(&data), EncryptionStatus::FileEncrypted);
    }

    #[test]
    fn rar14_not_encrypted() {
        let mut data = build_rar14_archive_header(0);
        data.extend_from_slice(&build_rar14_file_header(0));
        assert_eq!(detect_encryption(&data), EncryptionStatus::None);
    }

    #[test]
    fn rar14_insufficient_data() {
        let data = RAR14_SIGNATURE.to_vec();
        assert!(matches!(
            detect_encryption(&data),
            EncryptionStatus::Insufficient { .. }
        ));
    }

    #[test]
    fn sfx_rar14_file_encrypted_requires_rsfx_marker_like_rar_behavior() {
        let mut data = vec![0xAAu8; 64];
        data[28..32].copy_from_slice(&RAR14_SFX_MARKER);
        data.extend_from_slice(&build_rar14_archive_header(0));
        data.extend_from_slice(&build_rar14_file_header(0x04)); // LHD_PASSWORD
        assert_eq!(detect_encryption(&data), EncryptionStatus::FileEncrypted);
    }

    #[test]
    fn sfx_rar14_without_rsfx_marker_is_ignored_like_rar_behavior() {
        let mut data = vec![0xAAu8; 64];
        data.extend_from_slice(&build_rar14_archive_header(0));
        data.extend_from_slice(&build_rar14_file_header(0x04)); // LHD_PASSWORD
        assert_eq!(detect_encryption(&data), EncryptionStatus::NotRar);
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
