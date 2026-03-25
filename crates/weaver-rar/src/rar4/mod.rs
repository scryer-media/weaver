//! RAR4 archive support.
//!
//! RAR4 uses a completely different format from RAR5:
//! - 7-byte signature: `52 61 72 21 1A 07 00`
//! - Fixed-size header fields (u16/u32 LE) instead of variable-length integers
//! - Different compression algorithm (LZ77 with different Huffman tables)
//! - AES-128-CBC encryption (not AES-256)
//! - VM-based filters (we pattern-match standard filters instead of implementing a full VM)
//!
//! This module lives separately from the RAR5 code so it can be removed when
//! RAR4 becomes obsolete.

pub mod header;
pub mod types;

use std::io::{Read, Seek};

use tracing::{debug, warn};

use crate::error::{RarError, RarResult};
use crate::types::{CompressionInfo, CompressionMethod, HostOs, MemberInfo, VolumeSpan};
use types::*;

/// Parsed RAR4 volume contents.
#[derive(Debug)]
pub struct Rar4ParsedVolume {
    pub archive_header: Rar4ArchiveHeader,
    pub files: Vec<Rar4FileHeader>,
    pub end: Option<Rar4EndHeader>,
}

/// Parse all headers from a RAR4 archive volume.
///
/// Reader should be positioned right after the 7-byte RAR4 signature.
/// If the archive uses header-level encryption (`-hp`), the password is
/// required to decrypt headers.
pub fn parse_rar4_headers<R: Read + Seek>(
    reader: &mut R,
    password: Option<&str>,
) -> RarResult<Rar4ParsedVolume> {
    let mut archive_header = None;
    let mut files = Vec::new();
    let mut end = None;

    while let Some(raw) = header::read_raw_header(reader)? {
        match raw.header_type {
            Rar4HeaderType::Mark => {
                // Marker header — skip, it's just the signature confirmation.
            }
            Rar4HeaderType::Archive => {
                let arch = header::parse_archive_header(&raw)?;
                debug!(
                    "RAR4 archive: solid={} volume={} encrypted={}",
                    arch.is_solid, arch.is_volume, arch.is_encrypted
                );
                if arch.is_encrypted {
                    let pwd = password.ok_or(RarError::EncryptedArchive)?;
                    archive_header = Some(arch);

                    // Parse remaining headers — each has its own salt.
                    parse_rar4_encrypted_headers(reader, pwd, &mut files, &mut end)?;
                    break;
                }
                archive_header = Some(arch);
            }
            Rar4HeaderType::File => {
                let fh = header::parse_file_header(&raw)?;
                debug!(
                    "RAR4 file: name={:?} packed={} unpacked={} method={:?}",
                    fh.name, fh.packed_size, fh.unpacked_size, fh.method
                );
                // For files with LARGE flag, the raw header's data_area_size only
                // has the low 32 bits. Use the fully-resolved packed_size instead.
                let skip_size = fh.packed_size;
                files.push(fh);
                reader
                    .seek(std::io::SeekFrom::Current(skip_size as i64))
                    .map_err(RarError::Io)?;
                continue;
            }
            Rar4HeaderType::EndArchive => {
                let e = header::parse_end_header(&raw);
                debug!("RAR4 end: more_volumes={}", e.more_volumes);
                end = Some(e);
                break;
            }
            Rar4HeaderType::Comment
            | Rar4HeaderType::Extra
            | Rar4HeaderType::Sub
            | Rar4HeaderType::Recovery
            | Rar4HeaderType::NewSub => {
                debug!("RAR4 skipping header type {:?}", raw.header_type);
            }
            Rar4HeaderType::Unknown(t) => {
                warn!("RAR4 unknown header type {:#04x}", t);
                if raw.flags & types::common_flags::SKIP_IF_UNKNOWN == 0 {
                    return Err(RarError::CorruptArchive {
                        detail: format!("RAR4 unknown required header type {:#04x}", t),
                    });
                }
            }
        }

        // Skip data area if present.
        header::skip_data_area(reader, &raw)?;
    }

    let archive_header = archive_header.ok_or_else(|| RarError::CorruptArchive {
        detail: "RAR4 archive missing archive header".into(),
    })?;

    Ok(Rar4ParsedVolume {
        archive_header,
        files,
        end,
    })
}

/// Parse remaining RAR4 headers from an encrypted stream.
///
/// In RAR4 header encryption (`-hp`), each header is individually encrypted:
/// a fresh 8-byte salt precedes each header in the stream, and the decryptor
/// is re-initialized per header (arcread.cpp:157-164 in unrar).
fn parse_rar4_encrypted_headers<R: Read + Seek>(
    reader: &mut R,
    password: &str,
    files: &mut Vec<Rar4FileHeader>,
    end: &mut Option<Rar4EndHeader>,
) -> RarResult<()> {
    let kdf_cache = crate::crypto::KdfCache::new();

    loop {
        // Each encrypted header is preceded by its own 8-byte salt.
        let mut salt = [0u8; 8];
        match reader.read_exact(&mut salt) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(RarError::Io(e)),
        }

        let (key, iv) = kdf_cache.derive_key_rar4(password, &salt);
        let mut decryptor = crate::crypto::Rar4CbcDecryptor::new(&key, &iv);

        let raw = match header::read_raw_header_encrypted(reader, &mut decryptor)? {
            Some(raw) => raw,
            None => break,
        };

        match raw.header_type {
            Rar4HeaderType::File => {
                let mut fh = header::parse_file_header(&raw)?;
                // For encrypted headers, raw.offset is meaningless. The stream
                // is positioned right after the aligned encrypted header block,
                // which is where the file data starts.
                fh.data_offset = reader.stream_position().map_err(RarError::Io)?;
                debug!(
                    "RAR4 encrypted file: name={:?} packed={} unpacked={} method={:?}",
                    fh.name, fh.packed_size, fh.unpacked_size, fh.method
                );
                let skip_size = fh.packed_size;
                files.push(fh);
                reader
                    .seek(std::io::SeekFrom::Current(skip_size as i64))
                    .map_err(RarError::Io)?;
            }
            Rar4HeaderType::EndArchive => {
                let e = header::parse_end_header(&raw);
                debug!("RAR4 encrypted end: more_volumes={}", e.more_volumes);
                *end = Some(e);
                break;
            }
            _ => {
                debug!("RAR4 encrypted: skipping header type {:?}", raw.header_type);
                if raw.data_area_size > 0 {
                    reader
                        .seek(std::io::SeekFrom::Current(raw.data_area_size as i64))
                        .map_err(RarError::Io)?;
                }
            }
        }
    }
    Ok(())
}

/// Convert a RAR4 file header to the unified MemberInfo type.
pub fn to_member_info(fh: &Rar4FileHeader, volume_index: usize) -> MemberInfo {
    let host_os = match fh.host_os {
        Rar4HostOs::Windows | Rar4HostOs::MsDos | Rar4HostOs::Os2 => HostOs::Windows,
        Rar4HostOs::Unix | Rar4HostOs::MacOs | Rar4HostOs::BeOs => HostOs::Unix,
        Rar4HostOs::Unknown(v) => HostOs::Unknown(v as u64),
    };

    let method = match fh.method {
        Rar4Method::Store => CompressionMethod::Store,
        Rar4Method::Fastest => CompressionMethod::Fastest,
        Rar4Method::Fast => CompressionMethod::Fast,
        Rar4Method::Normal => CompressionMethod::Normal,
        Rar4Method::Good => CompressionMethod::Good,
        Rar4Method::Best => CompressionMethod::Best,
        Rar4Method::Unknown(c) => CompressionMethod::Unknown(c),
    };

    // Convert DOS datetime to SystemTime (simplified — just store as raw for now).
    // DOS datetime: bits 0-4=seconds/2, 5-10=minute, 11-15=hour (time word)
    //               bits 0-4=day, 5-8=month, 9-15=year-1980 (date word)
    let mtime = dos_datetime_to_system_time(fh.mtime);

    MemberInfo {
        name: fh.name.clone(),
        raw_name: fh.name.clone(),
        unpacked_size: Some(fh.unpacked_size),
        compressed_size: fh.packed_size,
        is_directory: fh.is_directory,
        crc32: Some(fh.crc32),
        mtime,
        host_os,
        compression: CompressionInfo {
            format: crate::types::ArchiveFormat::Rar4,
            version: 29,
            solid: fh.is_solid,
            method,
            dict_size: 4 * 1024 * 1024, // RAR4 default: 4 MB
        },
        is_encrypted: fh.is_encrypted,
        hash: None,
        volumes: VolumeSpan::single(volume_index),
        is_symlink: false,
        is_hardlink: false,
        link_target: None,
    }
}

/// Convert a DOS date/time u32 to SystemTime.
fn dos_datetime_to_system_time(dos_dt: u32) -> Option<std::time::SystemTime> {
    let time_part = (dos_dt & 0xFFFF) as u16;
    let date_part = (dos_dt >> 16) as u16;

    let second = ((time_part & 0x1F) * 2) as u32;
    let minute = ((time_part >> 5) & 0x3F) as u32;
    let hour = ((time_part >> 11) & 0x1F) as u32;

    let day = (date_part & 0x1F) as u32;
    let month = ((date_part >> 5) & 0x0F) as u32;
    let year = ((date_part >> 9) & 0x7F) as u32 + 1980;

    if day == 0 || month == 0 || month > 12 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    // Calculate days since Unix epoch (1970-01-01).
    // Simplified: count days using a basic year/month calculation.
    let days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = |y: u32| y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400));

    let mut total_days: i64 = 0;
    for y in 1970..year {
        total_days += if is_leap(y) { 366 } else { 365 };
    }
    for m in 1..month {
        total_days += days_in_month[m as usize] as i64;
        if m == 2 && is_leap(year) {
            total_days += 1;
        }
    }
    total_days += (day - 1) as i64;

    let total_seconds =
        total_days * 86400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;

    Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(total_seconds as u64))
}

/// Extract a stored (method 0x30) RAR4 file from its data segment.
///
/// `data` is the packed data, `unpacked_size` is the expected size.
pub fn extract_store(data: &[u8], unpacked_size: u64) -> RarResult<Vec<u8>> {
    let size = unpacked_size as usize;
    if data.len() < size {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR4 stored data too short: have {} bytes, need {}",
                data.len(),
                size
            ),
        });
    }
    Ok(data[..size].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal RAR4 archive with one stored file.
    fn build_minimal_rar4_archive(filename: &str, content: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();

        // -- Archive header (0x73) --
        let arch_flags: u16 = 0;
        let arch_header_size: u16 = 7 + 6; // common 7 + reserved 6
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16 placeholder
        buf.push(0x73); // type
        buf.extend_from_slice(&arch_flags.to_le_bytes());
        buf.extend_from_slice(&arch_header_size.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]); // reserved

        // -- File header (0x74) --
        let name_bytes = filename.as_bytes();
        let file_header_size: u16 = 7 + 25 + name_bytes.len() as u16;
        let file_flags: u16 = common_flags::HAS_DATA;
        let packed_size = content.len() as u32;
        let unpacked_size = content.len() as u32;
        let crc32 = {
            let mut h = crc32fast::Hasher::new();
            h.update(content);
            h.finalize()
        };

        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x74); // type
        buf.extend_from_slice(&file_flags.to_le_bytes());
        buf.extend_from_slice(&file_header_size.to_le_bytes());
        buf.extend_from_slice(&packed_size.to_le_bytes()); // packed size
        buf.extend_from_slice(&unpacked_size.to_le_bytes()); // unpacked size
        buf.push(3); // Unix
        buf.extend_from_slice(&crc32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // datetime
        buf.push(29); // version
        buf.push(0x30); // method: Store
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // attrs
        buf.extend_from_slice(name_bytes);

        // Data area
        buf.extend_from_slice(content);

        // -- End header (0x7B) --
        let end_header_size: u16 = 7;
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x7B); // type
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags
        buf.extend_from_slice(&end_header_size.to_le_bytes());

        buf
    }

    #[test]
    fn test_parse_rar4_headers() {
        let data = build_minimal_rar4_archive("hello.txt", b"Hello world");
        let mut cursor = Cursor::new(data);
        let vol = parse_rar4_headers(&mut cursor, None).unwrap();
        assert!(!vol.archive_header.is_solid);
        assert_eq!(vol.files.len(), 1);
        assert_eq!(vol.files[0].name, "hello.txt");
        assert_eq!(vol.files[0].unpacked_size, 11);
        assert!(vol.end.is_some());
    }

    #[test]
    fn test_to_member_info() {
        let data = build_minimal_rar4_archive("file.dat", b"data");
        let mut cursor = Cursor::new(data);
        let vol = parse_rar4_headers(&mut cursor, None).unwrap();
        let mi = to_member_info(&vol.files[0], 0);
        assert_eq!(mi.name, "file.dat");
        assert_eq!(mi.unpacked_size, Some(4));
    }

    #[test]
    fn test_extract_store() {
        let data = b"stored content here";
        let result = extract_store(data, data.len() as u64).unwrap();
        assert_eq!(&result, data);
    }

    #[test]
    fn test_dos_datetime() {
        // 2023-06-15 14:30:22
        // Date: year=43 month=6 day=15 -> (43 << 9) | (6 << 5) | 15 = 22095 = 0x564F
        // Time: hour=14 minute=30 second=11 -> (14 << 11) | (30 << 5) | 11 = 29451 = 0x730B
        let date_word: u16 = (43 << 9) | (6 << 5) | 15;
        let time_word: u16 = (14 << 11) | (30 << 5) | 11;
        let dos_dt = (date_word as u32) << 16 | time_word as u32;
        let st = dos_datetime_to_system_time(dos_dt);
        assert!(st.is_some());
    }

    #[test]
    fn test_dos_datetime_invalid() {
        // Month=0 is invalid
        assert!(dos_datetime_to_system_time(0).is_none());
    }

    #[test]
    fn test_parse_rar4_hp_encrypted_headers() {
        // Test against a real RAR4 -hp archive from e2e fixtures.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../e2e/testdata/rar4-encrypted/archive.rar");
        if !path.exists() {
            eprintln!("skipping test: e2e fixture not found at {}", path.display());
            return;
        }
        let data = std::fs::read(&path).unwrap();
        // Skip 7-byte signature.
        let mut cursor = Cursor::new(&data[7..]);
        let vol = parse_rar4_headers(&mut cursor, Some("e2e-test-password")).unwrap();
        assert!(vol.archive_header.is_encrypted);
        assert!(!vol.files.is_empty(), "should have parsed at least one file header");
        eprintln!("parsed {} files from -hp archive", vol.files.len());
        for f in &vol.files {
            eprintln!("  file: {:?} packed={} unpacked={} method={:?} encrypted={} salt={:?} flags={:#06x}",
                f.name, f.packed_size, f.unpacked_size, f.method, f.is_encrypted, f.salt, f.flags);
        }
    }

    #[test]
    fn test_parse_rar4_hp_no_password_returns_error() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../e2e/testdata/rar4-encrypted/archive.rar");
        if !path.exists() {
            return;
        }
        let data = std::fs::read(&path).unwrap();
        let mut cursor = Cursor::new(&data[7..]);
        let err = parse_rar4_headers(&mut cursor, None).unwrap_err();
        assert!(
            matches!(err, RarError::EncryptedArchive),
            "expected EncryptedArchive, got: {err}"
        );
    }
}
