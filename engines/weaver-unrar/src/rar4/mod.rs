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
use crate::types::{
    CompressionInfo, CompressionMethod, HostOs, MemberInfo, UnixOwnerInfo, VolumeSpan,
};
use types::*;

/// Parsed RAR4 volume contents.
#[derive(Debug)]
pub struct Rar4ParsedVolume {
    pub archive_header: Rar4ArchiveHeader,
    pub files: Vec<Rar4FileHeader>,
    pub services: Vec<Rar4FileHeader>,
    pub old_services: Vec<Rar4OldServiceHeader>,
    pub comments: Vec<Rar4CommentHeader>,
    pub recovery_records: Vec<Rar4RecoveryRecord>,
    pub end: Option<Rar4EndHeader>,
}

/// Parse all headers from a RAR 1.4 archive volume.
///
/// Reader must be positioned at the `RE~^` main-header marker.
pub fn parse_rar14_headers<R: Read + Seek>(reader: &mut R) -> RarResult<Rar4ParsedVolume> {
    let main_offset = reader.stream_position().map_err(RarError::Io)?;
    let mut main = [0u8; 7];
    reader.read_exact(&mut main).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            RarError::InvalidSignature
        } else {
            RarError::Io(e)
        }
    })?;

    let (mut archive_header, main_size) = header::parse_rar14_archive_header(&main)?;
    let mut comments = Vec::new();
    if main_size > 7 {
        let mut main_body = vec![0u8; usize::from(main_size - 7)];
        reader.read_exact(&mut main_body).map_err(RarError::Io)?;
        if let Some(comment) =
            header::parse_rar14_main_comment(&main_body, main_offset, archive_header.flags)?
        {
            comments.push(comment);
        }
    }

    let mut files = Vec::new();
    loop {
        let offset = reader.stream_position().map_err(RarError::Io)?;
        let mut fixed = [0u8; 21];
        match reader.read_exact(&mut fixed) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(RarError::Io(e)),
        }

        let head_size = u16::from_le_bytes([fixed[10], fixed[11]]);
        if head_size < 21 {
            return Err(RarError::CorruptArchive {
                detail: format!("RAR14 file header size {head_size} is less than 21"),
            });
        }
        let mut data = fixed.to_vec();
        let extra = usize::from(head_size - 21);
        if extra > 0 {
            let start = data.len();
            data.resize(start + extra, 0);
            reader.read_exact(&mut data[start..]).map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    RarError::TruncatedHeader { offset }
                } else {
                    RarError::Io(e)
                }
            })?;
        }

        let Some(fh) = header::parse_rar14_file_header(&data, offset)? else {
            break;
        };
        let skip_size = fh.packed_size;
        files.push(fh);
        reader
            .seek(std::io::SeekFrom::Current(skip_size as i64))
            .map_err(RarError::Io)?;
    }

    if archive_header.is_volume {
        archive_header.is_first_volume = files.first().is_none_or(|file| !file.split_before);
    }

    Ok(Rar4ParsedVolume {
        archive_header,
        files,
        services: Vec::new(),
        old_services: Vec::new(),
        comments,
        recovery_records: Vec::new(),
        end: None,
    })
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
    let kdf_cache = crate::crypto::KdfCache::new();
    parse_rar4_headers_with_kdf_cache(reader, password, &kdf_cache)
}

pub(crate) fn parse_rar4_headers_with_kdf_cache<R: Read + Seek>(
    reader: &mut R,
    password: Option<&str>,
    kdf_cache: &crate::crypto::KdfCache,
) -> RarResult<Rar4ParsedVolume> {
    let mut archive_header = None;
    let mut files = Vec::new();
    let mut services = Vec::new();
    let mut old_services = Vec::new();
    let mut comments = Vec::new();
    let mut recovery_records = Vec::new();
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
                    parse_rar4_encrypted_headers(
                        reader,
                        pwd,
                        &mut files,
                        &mut services,
                        &mut old_services,
                        &mut end,
                        kdf_cache,
                    )?;
                    break;
                }
                archive_header = Some(arch);
            }
            Rar4HeaderType::File => {
                let fh = header::parse_file_header(&raw)?;
                debug!(
                    "RAR4 file: name={:?} packed={} unpacked={:?} method={:?}",
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
            Rar4HeaderType::Recovery => {
                let recovery = header::parse_recovery_header(&raw)?;
                debug!(
                    "RAR4 recovery: sectors={} blocks={} data_size={}",
                    recovery.recovery_sectors, recovery.total_blocks, recovery.data_size
                );
                recovery_records.push(recovery);
            }
            Rar4HeaderType::NewSub => {
                let service = header::parse_file_header(&raw)?;
                debug!(
                    "RAR4 service: name={:?} packed={} unpacked={:?} method={:?}",
                    service.name, service.packed_size, service.unpacked_size, service.method
                );
                attach_rar4_uowner_to_previous_file(&service, &mut files);
                let skip_size = service.packed_size;
                services.push(service);
                reader
                    .seek(std::io::SeekFrom::Current(skip_size as i64))
                    .map_err(RarError::Io)?;
                continue;
            }
            Rar4HeaderType::Comment => {
                let comment = header::parse_comment_header(&raw)?;
                debug!(
                    "RAR4 old comment: packed={} unpacked={} method={:?}",
                    comment.packed_size, comment.unpacked_size, comment.method
                );
                comments.push(comment);
            }
            Rar4HeaderType::Extra | Rar4HeaderType::Sub => {
                if raw.header_type == Rar4HeaderType::Sub {
                    let old_service = header::parse_old_service_header(&raw)?;
                    debug!(
                        "RAR4 old service: subtype={:#x} level={} data_size={}",
                        old_service.subtype, old_service.level, old_service.data_size
                    );
                    old_services.push(old_service);
                } else {
                    debug!("RAR4 skipping header type {:?}", raw.header_type);
                }
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

    let mut archive_header = archive_header.ok_or_else(|| RarError::CorruptArchive {
        detail: "RAR4 archive missing archive header".into(),
    })?;
    if archive_header.is_volume {
        if let Some(first_file) = files.first() {
            archive_header.is_first_volume = !first_file.split_before;
        } else if let Some(last_service) = services.last() {
            archive_header.is_first_volume = !last_service.split_before;
        }
    }

    Ok(Rar4ParsedVolume {
        archive_header,
        files,
        services,
        old_services,
        comments,
        recovery_records,
        end,
    })
}

fn attach_rar4_uowner_to_previous_file(service: &Rar4FileHeader, files: &mut [Rar4FileHeader]) {
    if service.name != "UOW" {
        return;
    }
    let Some(owner) = service
        .service_subdata
        .as_deref()
        .and_then(parse_rar4_uowner_subdata)
    else {
        return;
    };
    if let Some(file) = files.last_mut() {
        file.owner = Some(owner);
    }
}

pub(crate) fn parse_rar4_uowner_subdata(data: &[u8]) -> Option<UnixOwnerInfo> {
    let separator = data.iter().position(|byte| *byte == 0)?;
    let owner = &data[..separator];
    let group = &data[separator + 1..];
    let group_end = group
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(group.len());
    if owner.is_empty() || group_end == 0 {
        return None;
    }
    let group = &group[..group_end];
    let user_name = Some(String::from_utf8_lossy(owner).into_owned());
    let group_name = Some(String::from_utf8_lossy(group).into_owned());

    Some(UnixOwnerInfo {
        user_name,
        group_name,
        user_name_raw: Some(owner.to_vec()),
        group_name_raw: Some(group.to_vec()),
        uid: None,
        gid: None,
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
    services: &mut Vec<Rar4FileHeader>,
    old_services: &mut Vec<Rar4OldServiceHeader>,
    end: &mut Option<Rar4EndHeader>,
    kdf_cache: &crate::crypto::KdfCache,
) -> RarResult<()> {
    loop {
        // Each encrypted header is preceded by its own 8-byte salt.
        let mut salt = [0u8; 8];
        match reader.read_exact(&mut salt) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(RarError::Io(e)),
        }

        let (key, iv) = kdf_cache.derive_key_rar4(password, Some(&salt));
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
                    "RAR4 encrypted file: name={:?} packed={} unpacked={:?} method={:?}",
                    fh.name, fh.packed_size, fh.unpacked_size, fh.method
                );
                let skip_size = fh.packed_size;
                files.push(fh);
                reader
                    .seek(std::io::SeekFrom::Current(skip_size as i64))
                    .map_err(RarError::Io)?;
            }
            Rar4HeaderType::NewSub => {
                let mut service = header::parse_file_header(&raw)?;
                service.data_offset = reader.stream_position().map_err(RarError::Io)?;
                debug!(
                    "RAR4 encrypted service: name={:?} packed={} unpacked={:?} method={:?}",
                    service.name, service.packed_size, service.unpacked_size, service.method
                );
                attach_rar4_uowner_to_previous_file(&service, files);
                let skip_size = service.packed_size;
                services.push(service);
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
            Rar4HeaderType::Sub => {
                let old_service = header::parse_old_service_header(&raw)?;
                debug!(
                    "RAR4 encrypted old service: subtype={:#x} level={} data_size={}",
                    old_service.subtype, old_service.level, old_service.data_size
                );
                let skip_size = old_service.data_size;
                old_services.push(old_service);
                if skip_size > 0 {
                    reader
                        .seek(std::io::SeekFrom::Current(skip_size as i64))
                        .map_err(RarError::Io)?;
                }
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
    to_member_info_with_archive_solid(fh, volume_index, false)
}

/// Convert a RAR4 file header using the archive-level solid flag.
///
/// UnRAR treats RAR 1.3 through 1.5 solidness as archive-global because those
/// formats do not reliably store a per-file solid bit.
pub fn to_member_info_with_archive_solid(
    fh: &Rar4FileHeader,
    volume_index: usize,
    archive_solid: bool,
) -> MemberInfo {
    let host_os = match fh.host_os {
        Rar4HostOs::Unix | Rar4HostOs::BeOs => HostOs::Unix,
        Rar4HostOs::Windows | Rar4HostOs::MsDos | Rar4HostOs::Os2 | Rar4HostOs::MacOs => {
            HostOs::Windows
        }
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

    let mtime = fh
        .mtime_precise
        .or_else(|| dos_datetime_to_system_time(fh.mtime));

    MemberInfo {
        name: fh.name.clone(),
        raw_name: fh.name.clone(),
        raw_name_bytes: fh.name_raw.clone(),
        unpacked_size: fh.unpacked_size,
        compressed_size: fh.packed_size,
        is_directory: fh.is_directory,
        crc32: (!fh.is_rar14).then_some(fh.crc32),
        mtime,
        ctime: fh.ctime,
        atime: fh.atime,
        version: fh.version,
        host_os,
        compression: CompressionInfo {
            format: if fh.is_rar14 {
                crate::types::ArchiveFormat::Rar14
            } else {
                crate::types::ArchiveFormat::Rar4
            },
            version: fh.unpack_version,
            solid: fh.is_solid || (archive_solid && fh.unpack_version <= 15),
            method,
            dict_size: fh.dict_size,
        },
        is_encrypted: fh.is_encrypted,
        hash: None,
        attributes: crate::types::FileAttributes(fh.attributes as u64),
        owner: fh.owner.clone(),
        volumes: VolumeSpan::single(volume_index),
        is_symlink: fh.is_unix_symlink,
        is_hardlink: false,
        is_file_copy: false,
        link_target: None,
        link_target_bytes: None,
    }
}

/// Convert a DOS date/time u32 to SystemTime.
pub(crate) fn dos_datetime_to_system_time(dos_dt: u32) -> Option<std::time::SystemTime> {
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

    fn finalize_header_crc(header: &mut [u8]) {
        let crc16 = (crc32fast::hash(&header[2..]) & 0xFFFF) as u16;
        header[0..2].copy_from_slice(&crc16.to_le_bytes());
    }

    fn build_minimal_rar4_archive_with_flags(
        filename: &str,
        content: &[u8],
        arch_flags: u16,
        file_header_extra_flags: u16,
    ) -> Vec<u8> {
        let mut buf = Vec::new();

        // -- Archive header (0x73) --
        let arch_header_size: u16 = 7 + 6; // common 7 + reserved 6
        let arch_start = buf.len();
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16 placeholder
        buf.push(0x73); // type
        buf.extend_from_slice(&arch_flags.to_le_bytes());
        buf.extend_from_slice(&arch_header_size.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]); // reserved
        finalize_header_crc(&mut buf[arch_start..]);

        // -- File header (0x74) --
        let name_bytes = filename.as_bytes();
        let file_header_size: u16 = 7 + 25 + name_bytes.len() as u16;
        let file_flags: u16 = common_flags::HAS_DATA | file_header_extra_flags;
        let packed_size = content.len() as u32;
        let unpacked_size = content.len() as u32;
        let crc32 = {
            let mut h = crc32fast::Hasher::new();
            h.update(content);
            h.finalize()
        };

        let file_start = buf.len();
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
        finalize_header_crc(&mut buf[file_start..]);

        // Data area
        buf.extend_from_slice(content);

        // -- End header (0x7B) --
        let end_header_size: u16 = 7;
        let end_start = buf.len();
        buf.extend_from_slice(&[0x00, 0x00]); // CRC16
        buf.push(0x7B); // type
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags
        buf.extend_from_slice(&end_header_size.to_le_bytes());
        finalize_header_crc(&mut buf[end_start..]);

        buf
    }

    /// Build a minimal RAR4 archive with one stored file.
    fn build_minimal_rar4_archive(filename: &str, content: &[u8]) -> Vec<u8> {
        build_minimal_rar4_archive_with_flags(filename, content, 0, 0)
    }

    #[test]
    fn test_parse_rar4_headers() {
        let data = build_minimal_rar4_archive("hello.txt", b"Hello world");
        let mut cursor = Cursor::new(data);
        let vol = parse_rar4_headers(&mut cursor, None).unwrap();
        assert!(!vol.archive_header.is_solid);
        assert_eq!(vol.files.len(), 1);
        assert_eq!(vol.files[0].name, "hello.txt");
        assert_eq!(vol.files[0].unpacked_size, Some(11));
        assert!(vol.end.is_some());
    }

    #[test]
    fn test_parse_rar4_headers_derives_first_volume_from_first_file_like_unrar() {
        let data = build_minimal_rar4_archive_with_flags(
            "hello.txt",
            b"Hello world",
            archive_flags::VOLUME,
            0,
        );
        let mut cursor = Cursor::new(data);
        let vol = parse_rar4_headers(&mut cursor, None).unwrap();

        assert!(vol.archive_header.is_volume);
        assert!(vol.archive_header.is_first_volume);
    }

    #[test]
    fn test_parse_rar4_headers_split_first_file_is_not_first_volume_like_unrar() {
        let data = build_minimal_rar4_archive_with_flags(
            "hello.txt",
            b"Hello world",
            archive_flags::VOLUME | archive_flags::FIRST_VOLUME,
            file_flags::SPLIT_BEFORE,
        );
        let mut cursor = Cursor::new(data);
        let vol = parse_rar4_headers(&mut cursor, None).unwrap();

        assert!(vol.archive_header.is_volume);
        assert!(!vol.archive_header.is_first_volume);
    }

    #[test]
    fn test_parse_rar4_headers_collects_old_service_records() {
        let mut data = build_minimal_rar4_archive("hello.txt", b"Hello world");
        data.truncate(data.len() - 7); // Drop the end header and append it again later.

        let service_start = data.len();
        let service_name = b":stream";
        let mut service_body = Vec::new();
        service_body.extend_from_slice(&3u32.to_le_bytes()); // data size
        service_body.extend_from_slice(&0x105u16.to_le_bytes()); // STREAM_HEAD
        service_body.push(1); // level
        service_body.extend_from_slice(&7u32.to_le_bytes()); // unpacked size
        service_body.push(20); // unpack version
        service_body.push(0x30); // method
        service_body.extend_from_slice(&0x5566_7788u32.to_le_bytes());
        service_body.extend_from_slice(&(service_name.len() as u16).to_le_bytes());
        service_body.extend_from_slice(service_name);
        data.extend_from_slice(&[0, 0]);
        data.push(0x77);
        data.extend_from_slice(&common_flags::HAS_DATA.to_le_bytes());
        data.extend_from_slice(&((7 + service_body.len()) as u16).to_le_bytes());
        data.extend_from_slice(&service_body);
        finalize_header_crc(&mut data[service_start..]);
        data.extend_from_slice(b"abc");

        let end_start = data.len();
        data.extend_from_slice(&[0, 0]);
        data.push(0x7b);
        data.extend_from_slice(&0u16.to_le_bytes());
        data.extend_from_slice(&7u16.to_le_bytes());
        finalize_header_crc(&mut data[end_start..]);

        let mut cursor = Cursor::new(data);
        let vol = parse_rar4_headers(&mut cursor, None).unwrap();
        assert_eq!(vol.old_services.len(), 1);
        assert!(vol.end.is_some());
        match &vol.old_services[0].data {
            Rar4OldServiceData::Stream {
                unpacked_size,
                stream_name,
                ..
            } => {
                assert_eq!(*unpacked_size, 7);
                assert_eq!(stream_name, ":stream");
            }
            other => panic!("expected old stream service, got {other:?}"),
        }
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
    fn test_to_member_info_with_archive_solid_matches_unrar_for_rar15() {
        let mut fh = Rar4FileHeader {
            is_rar14: false,
            flags: 0,
            packed_size: 0,
            unpacked_size: Some(0),
            host_os: Rar4HostOs::Unix,
            crc32: 0,
            mtime: 0,
            mtime_precise: None,
            ctime: None,
            atime: None,
            version: None,
            unpack_version: 15,
            method: Rar4Method::Normal,
            dict_size: 0x10000,
            name: "old-solid.txt".to_string(),
            name_raw: Some(b"old-solid.txt".to_vec()),
            is_directory: false,
            is_unix_symlink: false,
            is_encrypted: false,
            encryption_method: None,
            is_solid: false,
            is_subblock: false,
            split_before: false,
            split_after: false,
            comment_in_header: false,
            data_offset: 0,
            salt: None,
            attributes: 0,
            service_subdata: None,
            owner: None,
        };

        assert!(
            to_member_info_with_archive_solid(&fh, 0, true)
                .compression
                .solid
        );
        assert!(!to_member_info(&fh, 0).compression.solid);

        fh.unpack_version = 20;
        assert!(
            !to_member_info_with_archive_solid(&fh, 0, true)
                .compression
                .solid
        );
    }

    #[test]
    fn test_to_member_info_preserves_rar4_extended_times() {
        let mtime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(10);
        let ctime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(20);
        let atime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(30);
        let fh = Rar4FileHeader {
            is_rar14: false,
            flags: file_flags::EXT_TIME,
            packed_size: 0,
            unpacked_size: Some(0),
            host_os: Rar4HostOs::Unix,
            crc32: 0,
            mtime: 0,
            mtime_precise: Some(mtime),
            ctime: Some(ctime),
            atime: Some(atime),
            version: Some(12),
            unpack_version: 29,
            method: Rar4Method::Store,
            dict_size: 0,
            name: "times.txt".to_string(),
            name_raw: Some(b"times.txt".to_vec()),
            is_directory: false,
            is_unix_symlink: false,
            is_encrypted: false,
            encryption_method: None,
            is_solid: false,
            is_subblock: false,
            split_before: false,
            split_after: false,
            comment_in_header: false,
            data_offset: 0,
            salt: None,
            attributes: 0,
            service_subdata: None,
            owner: None,
        };

        let mi = to_member_info(&fh, 0);
        assert_eq!(mi.raw_name_bytes.as_deref(), Some(&b"times.txt"[..]));
        assert_eq!(mi.mtime, Some(mtime));
        assert_eq!(mi.ctime, Some(ctime));
        assert_eq!(mi.atime, Some(atime));
        assert_eq!(mi.version, Some(12));
    }

    #[test]
    fn test_to_member_info_host_system_matches_unrar_rar4_classification() {
        let mut fh = Rar4FileHeader {
            is_rar14: false,
            flags: 0,
            packed_size: 0,
            unpacked_size: Some(0),
            host_os: Rar4HostOs::Unix,
            crc32: 0,
            mtime: 0,
            mtime_precise: None,
            ctime: None,
            atime: None,
            version: None,
            unpack_version: 29,
            method: Rar4Method::Store,
            dict_size: 0,
            name: "host.txt".to_string(),
            name_raw: Some(b"host.txt".to_vec()),
            is_directory: false,
            is_unix_symlink: false,
            is_encrypted: false,
            encryption_method: None,
            is_solid: false,
            is_subblock: false,
            split_before: false,
            split_after: false,
            comment_in_header: false,
            data_offset: 0,
            salt: None,
            attributes: 0,
            service_subdata: None,
            owner: None,
        };

        for host in [
            Rar4HostOs::MsDos,
            Rar4HostOs::Os2,
            Rar4HostOs::Windows,
            Rar4HostOs::MacOs,
        ] {
            fh.host_os = host;
            assert_eq!(to_member_info(&fh, 0).host_os, HostOs::Windows);
        }

        for host in [Rar4HostOs::Unix, Rar4HostOs::BeOs] {
            fh.host_os = host;
            assert_eq!(to_member_info(&fh, 0).host_os, HostOs::Unix);
        }

        fh.host_os = Rar4HostOs::Unknown(42);
        assert_eq!(to_member_info(&fh, 0).host_os, HostOs::Unknown(42));
    }

    #[test]
    fn test_parse_rar4_uowner_preserves_raw_bytes_for_lookup_like_unrar() {
        let owner = parse_rar4_uowner_subdata(b"al\xffice\0gr\xf0up").unwrap();

        assert_eq!(owner.user_name_raw.as_deref(), Some(&b"al\xffice"[..]));
        assert_eq!(owner.group_name_raw.as_deref(), Some(&b"gr\xf0up"[..]));
        assert_eq!(owner.user_name.as_deref(), Some("al\u{fffd}ice"));
        assert_eq!(owner.group_name.as_deref(), Some("gr\u{fffd}up"));
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
        // Test against a real RAR4 -hp archive from local Weaver fixtures.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rar4/rar4_hp_large.rar");
        if !path.exists() {
            eprintln!("skipping test: e2e fixture not found at {}", path.display());
            return;
        }
        let data = std::fs::read(&path).unwrap();
        // Skip 7-byte signature.
        let mut cursor = Cursor::new(&data[7..]);
        let vol = parse_rar4_headers(&mut cursor, Some("e2e-test-password")).unwrap();
        assert!(vol.archive_header.is_encrypted);
        assert!(
            !vol.files.is_empty(),
            "should have parsed at least one file header"
        );
        eprintln!("parsed {} files from -hp archive", vol.files.len());
        for f in &vol.files {
            eprintln!(
                "  file: {:?} packed={} unpacked={:?} method={:?} encrypted={} salt={:?} flags={:#06x}",
                f.name, f.packed_size, f.unpacked_size, f.method, f.is_encrypted, f.salt, f.flags
            );
        }
    }

    #[test]
    fn test_parse_rar4_hp_encrypted_headers_uses_shared_kdf_cache() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rar4/rar4_hp_large.rar");
        if !path.exists() {
            eprintln!("skipping test: e2e fixture not found at {}", path.display());
            return;
        }
        let data = std::fs::read(&path).unwrap();
        let cache = crate::crypto::KdfCache::new();
        assert_eq!(cache.rar4_cached_entry_count(), 0);

        let mut cursor = Cursor::new(&data[7..]);
        let vol = parse_rar4_headers_with_kdf_cache(&mut cursor, Some("e2e-test-password"), &cache)
            .unwrap();

        assert!(vol.archive_header.is_encrypted);
        assert!(!vol.files.is_empty());
        assert!(
            cache.rar4_cached_entry_count() > 0,
            "encrypted header parsing should populate the caller-provided RAR4 KDF cache"
        );
    }

    #[test]
    fn test_parse_rar4_hp_no_password_returns_error() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rar4/rar4_hp_large.rar");
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
