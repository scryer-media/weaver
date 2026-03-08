//! RAR archive signature detection.
//!
//! RAR5 signature: 8 bytes `52 61 72 21 1A 07 01 00`
//! RAR4 signature: 7 bytes `52 61 72 21 1A 07 00`
//!
//! SFX (self-extracting) archives have an executable stub prepended before the
//! RAR signature. [`read_signature`] handles this by falling back to a bounded
//! scan when the signature is not found at the current reader position.

use std::io::{Read, Seek, SeekFrom};

use crate::error::{RarError, RarResult};
use crate::types::ArchiveFormat;

/// RAR5 magic bytes.
pub const RAR5_SIGNATURE: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

/// RAR4 magic bytes.
pub const RAR4_SIGNATURE: [u8; 7] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00];

/// Length of the RAR5 signature.
pub const RAR5_SIGNATURE_LEN: usize = 8;

/// Default maximum number of bytes to scan for an SFX signature (1 MB).
pub const DEFAULT_SFX_MAX_SCAN: u64 = 1_048_576;

/// Size of each read chunk when scanning for a signature.
const SCAN_CHUNK_SIZE: usize = 8192;

/// Detect the archive format from the first 8 bytes.
///
/// Returns `ArchiveFormat::Rar5` for RAR5 archives.
/// Returns `ArchiveFormat::Rar4` for RAR4 archives.
/// Returns `Err(InvalidSignature)` for unrecognized data.
pub fn detect_format(header: &[u8]) -> RarResult<ArchiveFormat> {
    if header.len() >= 8 && header[..8] == RAR5_SIGNATURE {
        return Ok(ArchiveFormat::Rar5);
    }
    if header.len() >= 7 && header[..7] == RAR4_SIGNATURE {
        return Ok(ArchiveFormat::Rar4);
    }
    Err(RarError::InvalidSignature)
}

/// Scan for a RAR signature within the first `max_scan` bytes.
///
/// Reads in chunks and searches for both the RAR5 and RAR4 signature byte
/// patterns. Handles the case where a signature straddles a chunk boundary
/// by keeping an overlap region between consecutive reads.
///
/// Returns the byte offset where the signature starts, or `None` if no
/// signature is found within the scan range.
fn scan_for_signature<R: Read + Seek>(reader: &mut R, max_scan: u64) -> RarResult<Option<u64>> {
    reader.seek(SeekFrom::Start(0)).map_err(RarError::Io)?;

    // The longest signature is RAR5 at 8 bytes. We need an overlap of 7 bytes
    // (signature length minus one) to catch signatures straddling chunk boundaries.
    let overlap = RAR5_SIGNATURE.len() - 1; // 7
    let mut buf = vec![0u8; SCAN_CHUNK_SIZE + overlap];
    let mut file_offset: u64 = 0;
    let mut carry = 0usize; // how many bytes carried over from previous chunk

    loop {
        // Don't scan beyond max_scan
        if file_offset >= max_scan {
            return Ok(None);
        }

        let remaining = (max_scan - file_offset) as usize;
        let to_read = remaining.min(SCAN_CHUNK_SIZE);
        let read_buf = &mut buf[carry..carry + to_read];

        let n = reader.read(read_buf).map_err(RarError::Io)?;
        if n == 0 {
            return Ok(None);
        }

        let window_len = carry + n;
        let window = &buf[..window_len];

        // Search for RAR5 signature (8 bytes)
        if let Some(pos) = find_subsequence(window, &RAR5_SIGNATURE) {
            let absolute = file_offset - carry as u64 + pos as u64;
            return Ok(Some(absolute));
        }

        // Search for RAR4 signature (7 bytes)
        if let Some(pos) = find_subsequence(window, &RAR4_SIGNATURE) {
            let absolute = file_offset - carry as u64 + pos as u64;
            return Ok(Some(absolute));
        }

        // Advance: keep the last `overlap` bytes for the next iteration
        file_offset += n as u64;

        if window_len > overlap {
            buf.copy_within(window_len - overlap..window_len, 0);
            carry = overlap;
        } else {
            // The entire window is smaller than the overlap; keep it all.
            carry = window_len;
        }
    }
}

/// Find the first occurrence of `needle` in `haystack`.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Read and validate the signature from a reader. Advances past the signature.
///
/// First tries to read the signature at the current position. If that fails
/// with `InvalidSignature`, performs a bounded scan (up to 1 MB) to support
/// SFX (self-extracting) archives where an executable stub precedes the RAR
/// data. On success the reader is positioned immediately after the signature.
pub fn read_signature<R: Read + Seek>(reader: &mut R) -> RarResult<ArchiveFormat> {
    let start = reader.stream_position().map_err(RarError::Io)?;

    let mut buf = [0u8; 8];
    let direct_result = reader.read_exact(&mut buf).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            RarError::InvalidSignature
        } else {
            RarError::Io(e)
        }
    });

    if let Ok(()) = direct_result {
        match detect_format(&buf) {
            Ok(ArchiveFormat::Rar4) => {
                // RAR4 signature is 7 bytes but we read 8; seek back 1 byte.
                reader.seek(SeekFrom::Current(-1)).map_err(RarError::Io)?;
                return Ok(ArchiveFormat::Rar4);
            }
            Ok(fmt) => return Ok(fmt),
            Err(RarError::InvalidSignature) => { /* fall through to SFX scan */ }
            Err(e) => return Err(e),
        }
    } else if let Err(RarError::InvalidSignature) = direct_result {
        // fall through to SFX scan
    } else {
        // Propagate I/O errors
        direct_result?;
    }

    // Attempt SFX scan from the beginning of the stream.
    match scan_for_signature(reader, DEFAULT_SFX_MAX_SCAN)? {
        Some(offset) => {
            // Seek to the offset and read the signature to determine format.
            reader.seek(SeekFrom::Start(offset)).map_err(RarError::Io)?;
            let mut sig_buf = [0u8; 8];
            reader.read_exact(&mut sig_buf).map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    RarError::InvalidSignature
                } else {
                    RarError::Io(e)
                }
            })?;
            let fmt = detect_format(&sig_buf)?;
            if fmt == ArchiveFormat::Rar4 {
                // RAR4 signature is 7 bytes but we read 8; seek back 1 byte.
                reader.seek(SeekFrom::Current(-1)).map_err(RarError::Io)?;
            }
            Ok(fmt)
        }
        None => {
            // Restore the original position before returning the error.
            let _ = reader.seek(SeekFrom::Start(start));
            Err(RarError::InvalidSignature)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_rar5_signature() {
        let result = detect_format(&RAR5_SIGNATURE);
        assert!(matches!(result, Ok(ArchiveFormat::Rar5)));
    }

    #[test]
    fn test_rar4_signature() {
        // RAR4 signature padded to 8 bytes
        let mut data = [0u8; 8];
        data[..7].copy_from_slice(&RAR4_SIGNATURE);
        let result = detect_format(&data);
        assert!(matches!(result, Ok(ArchiveFormat::Rar4)));
    }

    #[test]
    fn test_invalid_signature() {
        let data = [0x00; 8];
        let result = detect_format(&data);
        assert!(matches!(result, Err(RarError::InvalidSignature)));
    }

    #[test]
    fn test_too_short() {
        let data = [0x52, 0x61, 0x72];
        let result = detect_format(&data);
        assert!(matches!(result, Err(RarError::InvalidSignature)));
    }

    #[test]
    fn test_read_signature_rar5() {
        let mut cursor = Cursor::new(RAR5_SIGNATURE.to_vec());
        let result = read_signature(&mut cursor);
        assert!(matches!(result, Ok(ArchiveFormat::Rar5)));
    }

    #[test]
    fn test_read_signature_empty() {
        let mut cursor = Cursor::new(Vec::new());
        let result = read_signature(&mut cursor);
        assert!(matches!(result, Err(RarError::InvalidSignature)));
    }

    #[test]
    fn test_rar5_exact_match() {
        // Ensure the 8th byte matters
        let mut almost_rar5 = RAR5_SIGNATURE;
        almost_rar5[7] = 0x01; // Should be 0x00
        let result = detect_format(&almost_rar5);
        assert!(matches!(result, Err(RarError::InvalidSignature)));
    }

    // --- SFX tests ---

    #[test]
    fn test_sfx_rar5_signature() {
        // Prepend 1000 random-ish bytes (exe stub) before a valid RAR5 signature.
        let stub_size = 1000;
        let mut data = vec![0xCCu8; stub_size];
        data.extend_from_slice(&RAR5_SIGNATURE);
        data.extend_from_slice(&[0x00; 16]); // trailing data

        let mut cursor = Cursor::new(data);
        let result = read_signature(&mut cursor);
        assert!(matches!(result, Ok(ArchiveFormat::Rar5)));
        // Reader should be positioned right after the signature
        assert_eq!(cursor.position(), (stub_size + RAR5_SIGNATURE.len()) as u64);
    }

    #[test]
    fn test_sfx_rar4_signature() {
        // Prepend bytes before a RAR4 signature.
        let stub_size = 500;
        let mut data = vec![0xAAu8; stub_size];
        data.extend_from_slice(&RAR4_SIGNATURE);
        data.extend_from_slice(&[0x00; 16]);

        let mut cursor = Cursor::new(data);
        let result = read_signature(&mut cursor);
        assert!(matches!(result, Ok(ArchiveFormat::Rar4)));
    }

    #[test]
    fn test_sfx_exceeds_max_scan() {
        // Place the signature beyond the 1 MB scan limit.
        let stub_size = DEFAULT_SFX_MAX_SCAN as usize + 100;
        let mut data = vec![0xBBu8; stub_size];
        data.extend_from_slice(&RAR5_SIGNATURE);

        let mut cursor = Cursor::new(data);
        let result = read_signature(&mut cursor);
        assert!(matches!(result, Err(RarError::InvalidSignature)));
    }

    #[test]
    fn test_sfx_signature_straddles_chunk_boundary() {
        // Place the RAR5 signature so it starts in the last few bytes of a
        // chunk and continues into the next chunk. SCAN_CHUNK_SIZE is 8192,
        // and the overlap region is 7 bytes, so we place the signature
        // starting at offset (8192 - 3) so that 3 bytes are in the first
        // chunk and 5 bytes spill into the overlap/next read.
        let sig_offset = SCAN_CHUNK_SIZE - 3;
        let total_size = sig_offset + RAR5_SIGNATURE.len() + 16;
        let mut data = vec![0xDDu8; total_size];
        data[sig_offset..sig_offset + RAR5_SIGNATURE.len()]
            .copy_from_slice(&RAR5_SIGNATURE);

        let mut cursor = Cursor::new(data);
        let result = read_signature(&mut cursor);
        assert!(matches!(result, Ok(ArchiveFormat::Rar5)));
        assert_eq!(
            cursor.position(),
            (sig_offset + RAR5_SIGNATURE.len()) as u64
        );
    }

    #[test]
    fn test_normal_archive_still_works() {
        // Signature at byte 0 should work without scanning.
        let mut data = RAR5_SIGNATURE.to_vec();
        data.extend_from_slice(&[0x00; 32]);

        let mut cursor = Cursor::new(data);
        let result = read_signature(&mut cursor);
        assert!(matches!(result, Ok(ArchiveFormat::Rar5)));
        assert_eq!(cursor.position(), RAR5_SIGNATURE_LEN as u64);
    }

    #[test]
    fn test_scan_for_signature_returns_correct_offset() {
        let stub_size = 4096;
        let mut data = vec![0xEEu8; stub_size];
        data.extend_from_slice(&RAR5_SIGNATURE);
        data.extend_from_slice(&[0x00; 16]);

        let mut cursor = Cursor::new(data);
        let offset = scan_for_signature(&mut cursor, DEFAULT_SFX_MAX_SCAN)
            .expect("scan should not error");
        assert_eq!(offset, Some(stub_size as u64));
    }

    #[test]
    fn test_scan_for_signature_none_when_absent() {
        let data = vec![0xFFu8; 2048];
        let mut cursor = Cursor::new(data);
        let offset = scan_for_signature(&mut cursor, DEFAULT_SFX_MAX_SCAN)
            .expect("scan should not error");
        assert_eq!(offset, None);
    }
}
