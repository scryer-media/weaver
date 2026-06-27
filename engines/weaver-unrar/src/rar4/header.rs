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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::warn;

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

struct RawCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> RawCursor<'a> {
    fn new(data: &'a [u8], pos: usize) -> Self {
        Self { data, pos }
    }

    fn get1(&mut self) -> u8 {
        let value = self.data.get(self.pos).copied().unwrap_or(0);
        self.pos = self
            .pos
            .saturating_add(usize::from(self.pos < self.data.len()));
        value
    }

    fn get2(&mut self) -> u16 {
        if self.data.len().saturating_sub(self.pos) < 2 {
            return 0;
        }
        let value = read_u16(self.data, self.pos);
        self.pos += 2;
        value
    }

    fn get4(&mut self) -> u32 {
        if self.data.len().saturating_sub(self.pos) < 4 {
            return 0;
        }
        let value = read_u32(self.data, self.pos);
        self.pos += 4;
        value
    }

    fn get_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut bytes = vec![0; len];
        let available = self.data.get(self.pos..).unwrap_or(&[]);
        let copied = available.len().min(len);
        bytes[..copied].copy_from_slice(&available[..copied]);
        self.pos = self.pos.saturating_add(copied).min(self.data.len());
        bytes
    }
}

const OLD_SERVICE_NTACL: u16 = 0x104;
const OLD_SERVICE_STREAM: u16 = 0x105;
const MAX_STREAM_NAME20: usize = 260;
const SIZEOF_FILEHEAD3: usize = 32;

fn rar4_header_crc16(data: &[u8]) -> u16 {
    if data.len() <= 2 {
        return 0;
    }

    (crc32fast::hash(&data[2..]) & 0xFFFF) as u16
}

fn rar4_dictionary_size(flags: u16, is_directory: bool) -> u64 {
    if is_directory {
        0
    } else {
        0x1_0000u64 << ((flags & file_flags::WINDOW_MASK) >> 5)
    }
}

/// UnRAR's legacy checksum for RAR 1.4 archives.
pub(crate) fn checksum14_update(mut crc: u16, data: &[u8]) -> u16 {
    for &byte in data {
        crc = crc.wrapping_add(u16::from(byte));
        crc = crc.rotate_left(1);
    }
    crc
}

fn dos_datetime_to_system_time(dos_dt: u32) -> Option<SystemTime> {
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

    let days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = |y: u32| y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400));

    let mut total_days: i64 = 0;
    for y in 1970..year {
        total_days += if is_leap(y) { 366 } else { 365 };
    }
    for m in 1..month {
        total_days += days_in_month[m as usize];
        if m == 2 && is_leap(year) {
            total_days += 1;
        }
    }
    total_days += (day - 1) as i64;

    let total_seconds =
        total_days * 86400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;

    Some(UNIX_EPOCH + Duration::from_secs(total_seconds as u64))
}

fn parse_extended_times(
    data: &[u8],
    pos: usize,
    base_mtime: u32,
) -> (Option<SystemTime>, Option<SystemTime>, Option<SystemTime>) {
    let mut cursor = RawCursor::new(data, pos);
    let flags = cursor.get2();
    let mut times = [dos_datetime_to_system_time(base_mtime), None, None];

    for index in 0..3 {
        let rmode = flags >> ((3 - index) * 4);
        if rmode & 8 == 0 {
            continue;
        }

        if index != 0 {
            times[index] = dos_datetime_to_system_time(cursor.get4());
        }

        if let Some(mut time) = times[index] {
            if rmode & 4 != 0 {
                time = time + Duration::from_secs(1);
            }

            let mut hundred_ns = 0u32;
            let count = rmode & 3;
            for j in 0..count {
                hundred_ns |= u32::from(cursor.get1()) << ((j + 3 - count) * 8);
            }
            time = time + Duration::from_nanos(u64::from(hundred_ns) * 100);
            times[index] = Some(time);
        }
    }

    (times[0], times[1], times[2])
}

fn parse_version_file_name(name: &str) -> Option<u64> {
    let suffix = name.rsplit_once(';')?.1;
    let mut version = 0u64;
    let mut saw_digit = false;
    for byte in suffix.bytes() {
        if !byte.is_ascii_digit() {
            break;
        }
        saw_digit = true;
        version = version
            .saturating_mul(10)
            .saturating_add(u64::from(byte - b'0'));
    }
    (saw_digit && version != 0).then_some(version)
}

fn rar4_file_encryption_method(
    unpack_version: u8,
    is_encrypted: bool,
) -> Option<Rar4EncryptionMethod> {
    if !is_encrypted {
        return None;
    }

    Some(match unpack_version {
        13 => Rar4EncryptionMethod::Rar13,
        15 => Rar4EncryptionMethod::Rar15,
        20 | 26 => Rar4EncryptionMethod::Rar20,
        _ => Rar4EncryptionMethod::Rar30,
    })
}

fn is_rar4_unix_symlink(host_os: Rar4HostOs, attributes: u32) -> bool {
    matches!(host_os, Rar4HostOs::Unix) && (attributes & 0xF000) == 0xA000
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

    let crc16 = read_u16(&buf, 0);
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

    let actual_crc = rar4_header_crc16(&data);
    if actual_crc != crc16 {
        warn!(
            "RAR4 header CRC mismatch at offset {}: expected {:#06x}, got {:#06x}; parsing anyway like UnRAR",
            offset, crc16, actual_crc
        );
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
    let mut cursor = RawCursor::new(&raw.data, 7);
    let high_pos_av = cursor.get2();
    let pos_av = cursor.get4();
    Ok(Rar4ArchiveHeader {
        flags: raw.flags,
        is_volume: raw.flags & archive_flags::VOLUME != 0,
        is_solid: raw.flags & archive_flags::SOLID != 0,
        is_encrypted: raw.flags & archive_flags::ENCRYPTED_HEADERS != 0,
        has_recovery_record: raw.flags & archive_flags::RECOVERY != 0,
        is_locked: raw.flags & archive_flags::LOCK != 0,
        has_authenticity_verification: raw.flags & archive_flags::AUTH != 0,
        is_first_volume: raw.flags & archive_flags::FIRST_VOLUME != 0,
        high_pos_av,
        pos_av,
        is_signed: high_pos_av != 0 || pos_av != 0,
        comment_in_header: raw.flags & archive_flags::COMMENT != 0,
        new_naming: raw.flags & archive_flags::NEW_NUMBERING != 0,
    })
}

/// Parse a RAR 1.4 main header.
///
/// UnRAR's `ReadHeader14` treats the 4-byte `RE~^` marker as the beginning of
/// the main header, followed by `HeadSize:u16` and one byte of archive flags.
pub fn parse_rar14_archive_header(data: &[u8]) -> RarResult<(Rar4ArchiveHeader, u16)> {
    if data.len() < 7 || data[..4] != crate::signature::RAR14_SIGNATURE {
        return Err(RarError::InvalidSignature);
    }
    let head_size = read_u16(data, 4);
    if head_size < 7 {
        return Err(RarError::CorruptArchive {
            detail: format!("RAR14 main header size {head_size} is less than 7"),
        });
    }
    let flags = u16::from(data[6]);
    Ok((
        Rar4ArchiveHeader {
            flags,
            is_volume: flags & archive_flags::VOLUME != 0,
            is_solid: flags & archive_flags::SOLID != 0,
            is_encrypted: false,
            has_recovery_record: false,
            is_locked: flags & archive_flags::LOCK != 0,
            has_authenticity_verification: false,
            is_first_volume: false,
            high_pos_av: 0,
            pos_av: 0,
            is_signed: false,
            comment_in_header: flags & archive_flags::COMMENT != 0,
            new_naming: false,
        },
        head_size,
    ))
}

/// Parse the optional RAR 1.4 archive comment stored inside the main header.
///
/// UnRAR's `DoGetComment` reads `CmtLength:u16` immediately after the 7-byte
/// RAR14 main header. If `MHD_PACK_COMMENT` is set, the next `u16` is the
/// unpacked comment size and the remaining bytes are encrypted packed data.
pub fn parse_rar14_main_comment(
    main_body: &[u8],
    main_offset: u64,
    flags: u16,
) -> RarResult<Option<Rar4CommentHeader>> {
    if flags & archive_flags::COMMENT == 0 || main_body.len() < 2 {
        return Ok(None);
    }

    let comment_len = usize::from(read_u16(main_body, 0));
    if comment_len == 0 {
        return Ok(None);
    }

    let packed = flags & archive_flags::PACK_COMMENT != 0;
    let (data_start, packed_size, unpacked_size, unpack_version, method) = if packed {
        if comment_len < 2 || main_body.len() < 4 {
            return Ok(None);
        }
        let packed_size = comment_len - 2;
        let data_start = 4usize;
        if main_body.len().saturating_sub(data_start) < packed_size {
            return Err(RarError::TruncatedHeader {
                offset: main_offset,
            });
        }
        (
            data_start,
            packed_size,
            read_u16(main_body, 2),
            15,
            Rar4Method::Normal,
        )
    } else {
        let data_start = 2usize;
        if main_body.len().saturating_sub(data_start) < comment_len {
            return Err(RarError::TruncatedHeader {
                offset: main_offset,
            });
        }
        (
            data_start,
            comment_len,
            comment_len as u16,
            0,
            Rar4Method::Store,
        )
    };

    Ok(Some(Rar4CommentHeader {
        header_offset: main_offset,
        data_offset: main_offset + 7 + data_start as u64,
        packed_size: packed_size as u64,
        unpacked_size,
        unpack_version,
        method,
        crc16: 0,
    }))
}

/// Parse a RAR 1.4 file header body.
pub fn parse_rar14_file_header(data: &[u8], offset: u64) -> RarResult<Option<Rar4FileHeader>> {
    if data.len() < 21 {
        return Ok(None);
    }

    let packed_size = u64::from(read_u32(data, 0));
    let unpacked_size = Some(u64::from(read_u32(data, 4)));
    let checksum = read_u16(data, 8);
    let head_size = read_u16(data, 10);
    if head_size < 21 {
        return Err(RarError::CorruptArchive {
            detail: format!("RAR14 file header size {head_size} is less than 21"),
        });
    }
    let head_size_usize = usize::from(head_size);
    if data.len() < head_size_usize {
        return Err(RarError::TruncatedHeader { offset });
    }

    let mtime = read_u32(data, 12);
    let attributes = u32::from(data[16]);
    let flags = u16::from(data[17]) | common_flags::HAS_DATA;
    let unpack_version = if data[18] == 2 { 13 } else { 10 };
    let name_len = usize::from(data[19]);
    let method = Rar4Method::from(data[20]);
    if head_size_usize.saturating_sub(21) < name_len {
        return Err(RarError::TruncatedHeader { offset });
    }

    let name = decode_rar4_c_string_name(&data[21..21 + name_len]);
    let is_directory = attributes & 0x10 != 0;
    let is_encrypted = flags & file_flags::ENCRYPTED != 0;

    Ok(Some(Rar4FileHeader {
        is_rar14: true,
        flags,
        packed_size,
        unpacked_size,
        host_os: Rar4HostOs::MsDos,
        crc32: u32::from(checksum),
        mtime,
        mtime_precise: None,
        ctime: None,
        atime: None,
        version: None,
        unpack_version,
        method,
        dict_size: 0x10000,
        name,
        is_directory,
        is_unix_symlink: false,
        is_encrypted,
        encryption_method: is_encrypted.then_some(Rar4EncryptionMethod::Rar13),
        is_solid: false,
        is_subblock: false,
        split_before: flags & file_flags::SPLIT_BEFORE != 0,
        split_after: flags & file_flags::SPLIT_AFTER != 0,
        comment_in_header: flags & file_flags::COMMENT != 0,
        data_offset: offset + u64::from(head_size),
        salt: None,
        attributes,
        service_subdata: None,
        owner: None,
    }))
}

/// Parse a RAR4 file header from raw header data.
pub fn parse_file_header(raw: &RawRar4Header) -> RarResult<Rar4FileHeader> {
    let data = &raw.data;
    let mut cursor = RawCursor::new(data, 7); // skip common header
    let packed_low = u64::from(cursor.get4());
    let unpacked_low = cursor.get4();
    let host_os = Rar4HostOs::from(cursor.get1());
    let crc32 = cursor.get4();
    let mtime = cursor.get4();
    let unpack_version = cursor.get1();
    let method = Rar4Method::from(cursor.get1());
    let name_len = usize::from(cursor.get2());
    let attributes = cursor.get4();

    // High 32 bits of sizes (if LARGE flag set).
    let mut packed_size = packed_low;
    let mut unpacked_size = Some(unpacked_low as u64);
    let mut pos = cursor.pos;

    if raw.flags & file_flags::LARGE != 0 {
        let mut cursor = RawCursor::new(data, pos);
        let packed_high = u64::from(cursor.get4());
        let unpacked_high = cursor.get4();
        packed_size |= packed_high << 32;
        unpacked_size = if unpacked_low == u32::MAX && unpacked_high == u32::MAX {
            None
        } else {
            Some(((unpacked_high as u64) << 32) | unpacked_low as u64)
        };
        pos = cursor.pos;
    } else if unpacked_low == u32::MAX {
        unpacked_size = None;
    }

    // Read filename.
    let mut cursor = RawCursor::new(data, pos);
    let name_bytes = cursor.get_bytes(name_len);
    pos = cursor.pos;

    // Decode filename. If UNICODE flag, the name is encoded with RAR4's unicode
    // scheme. Otherwise, it's an OEM/ASCII name.
    let name = if raw.flags & file_flags::UNICODE != 0 {
        decode_rar4_unicode_name(&name_bytes)
    } else {
        decode_rar4_c_string_name(&name_bytes)
    };
    let version = if raw.flags & file_flags::VERSION != 0 {
        parse_version_file_name(&name)
    } else {
        None
    };

    let salt_len = if raw.flags & file_flags::SALT != 0 {
        8
    } else {
        0
    };
    let service_subdata = if raw.header_type == Rar4HeaderType::NewSub {
        let optional_len = usize::from(raw.header_size)
            .saturating_sub(SIZEOF_FILEHEAD3)
            .saturating_sub(name_len)
            .saturating_sub(salt_len);
        if optional_len > 0 {
            let mut cursor = RawCursor::new(data, pos);
            let subdata = cursor.get_bytes(optional_len);
            pos = cursor.pos;
            Some(subdata)
        } else {
            None
        }
    } else {
        None
    };

    // Read salt if encrypted.
    let salt = if raw.flags & file_flags::SALT != 0 {
        let mut cursor = RawCursor::new(data, pos);
        let mut s = [0u8; 8];
        s.copy_from_slice(&cursor.get_bytes(8));
        pos = cursor.pos;
        Some(s)
    } else {
        None
    };

    let (mtime_precise, ctime, atime) = if raw.flags & file_flags::EXT_TIME != 0 {
        parse_extended_times(data, pos, mtime)
    } else {
        (None, None, None)
    };

    // Match UnRAR's RAR4 handling: modern file headers encode directories in
    // the dictionary/window bits, not in the host-specific file attributes.
    let mut is_directory = raw.flags & file_flags::WINDOW_MASK == file_flags::DIRECTORY;
    if unpack_version < 20 && attributes & 0x10 != 0 {
        is_directory = true;
    }
    let dict_size = rar4_dictionary_size(raw.flags, is_directory);
    let is_unix_symlink = is_rar4_unix_symlink(host_os, attributes);

    let data_offset = raw.offset + raw.header_size as u64;
    let is_encrypted = raw.flags & file_flags::ENCRYPTED != 0;
    let file_block = raw.header_type == Rar4HeaderType::File;
    let is_subblock = !file_block && raw.flags & file_flags::SOLID != 0;

    Ok(Rar4FileHeader {
        is_rar14: false,
        flags: raw.flags,
        packed_size,
        unpacked_size,
        host_os,
        crc32,
        mtime,
        mtime_precise,
        ctime,
        atime,
        version,
        unpack_version,
        method,
        dict_size,
        name,
        is_directory,
        is_unix_symlink,
        is_encrypted,
        encryption_method: rar4_file_encryption_method(unpack_version, is_encrypted),
        is_solid: file_block && raw.flags & file_flags::SOLID != 0,
        is_subblock,
        split_before: raw.flags & file_flags::SPLIT_BEFORE != 0,
        split_after: raw.flags & file_flags::SPLIT_AFTER != 0,
        comment_in_header: raw.flags & file_flags::COMMENT != 0,
        data_offset,
        salt,
        attributes,
        service_subdata,
        owner: None,
    })
}

/// Parse a RAR4 end-of-archive header.
///
/// The ENDARC header may contain optional data after the 7-byte common header:
/// - If DATA_CRC flag (0x0002): 4 bytes archive data CRC
/// - If VOLUME_NUMBER flag (0x0004): 2 bytes volume number (0-based)
pub fn parse_end_header(raw: &RawRar4Header) -> Rar4EndHeader {
    let mut cursor = RawCursor::new(&raw.data, 7); // skip common header

    let has_data_crc = raw.flags & end_flags::DATA_CRC != 0;
    let data_crc = if has_data_crc {
        Some(cursor.get4())
    } else {
        None
    };

    let volume_number = if raw.flags & end_flags::VOLUME_NUMBER != 0 {
        Some(cursor.get2())
    } else {
        None
    };

    Rar4EndHeader {
        flags: raw.flags,
        more_volumes: raw.flags & end_flags::NEXT_VOLUME != 0,
        has_data_crc,
        rev_space: raw.flags & end_flags::REV_SPACE != 0,
        data_crc,
        volume_number,
    }
}

/// Parse a RAR4 recovery record (`HEAD3_PROTECT`).
///
/// This mirrors unrar's `ProtectHead` fields: data size, version,
/// recovery-sector count, total protected blocks, and the 8-byte marker.
pub fn parse_recovery_header(raw: &RawRar4Header) -> RarResult<Rar4RecoveryRecord> {
    let mut cursor = RawCursor::new(&raw.data, 7); // skip common header
    let data_size = u64::from(cursor.get4());
    let version = cursor.get1();
    let recovery_sectors = cursor.get2();
    let total_blocks = cursor.get4();
    let mut mark = [0u8; 8];
    mark.copy_from_slice(&cursor.get_bytes(8));

    Ok(Rar4RecoveryRecord {
        header_offset: raw.offset,
        data_offset: raw.offset + raw.header_size as u64,
        data_size,
        version,
        recovery_sectors,
        total_blocks,
        mark,
    })
}

/// Parse an old RAR 2.9 service header (`HEAD3_OLDSERVICE`).
///
/// UnRAR reads `DataSize:u32`, `SubType:u16`, `Level:u8`, then subtype-specific
/// fields for NT ACL and alternate stream records.
pub fn parse_old_service_header(raw: &RawRar4Header) -> RarResult<Rar4OldServiceHeader> {
    let mut cursor = RawCursor::new(&raw.data, 7); // skip common header
    let data_size = u64::from(cursor.get4());
    let subtype = cursor.get2();
    let level = cursor.get1();
    let service_data = match subtype {
        OLD_SERVICE_NTACL => Rar4OldServiceData::NtAcl {
            unpacked_size: cursor.get4(),
            unpack_version: cursor.get1(),
            method: Rar4Method::from(cursor.get1()),
            crc32: cursor.get4(),
        },
        OLD_SERVICE_STREAM => {
            let unpacked_size = cursor.get4();
            let unpack_version = cursor.get1();
            let method = Rar4Method::from(cursor.get1());
            let crc32 = cursor.get4();
            let stream_name_size = usize::from(cursor.get2()).min(MAX_STREAM_NAME20);
            let stream_name_bytes = cursor.get_bytes(stream_name_size);
            let nul_end = stream_name_bytes
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(stream_name_bytes.len());
            Rar4OldServiceData::Stream {
                unpacked_size,
                unpack_version,
                method,
                crc32,
                stream_name: crate::header::common::decode_utf8_prefix_until_nul(
                    &stream_name_bytes[..nul_end],
                ),
            }
        }
        _ => Rar4OldServiceData::Unknown,
    };

    Ok(Rar4OldServiceHeader {
        header_offset: raw.offset,
        data_offset: raw.offset + raw.header_size as u64,
        data_size,
        subtype,
        level,
        data: service_data,
    })
}

/// Parse an old-style RAR4/RAR2.9 comment header (`HEAD3_CMT`).
///
/// UnRAR's `CommHead` layout is the 7-byte short header followed by:
/// `UnpSize:u16`, `UnpVer:u8`, `Method:u8`, `CommCRC:u16`, then comment data.
pub fn parse_comment_header(raw: &RawRar4Header) -> RarResult<Rar4CommentHeader> {
    let mut cursor = RawCursor::new(&raw.data, 7); // skip common header
    let unpacked_size = cursor.get2();
    let unpack_version = cursor.get1();
    let method = Rar4Method::from(cursor.get1());
    let crc16 = cursor.get2();
    let data_offset = raw.offset + cursor.pos as u64;

    Ok(Rar4CommentHeader {
        header_offset: raw.offset,
        data_offset,
        packed_size: u64::from(raw.header_size).saturating_sub(cursor.pos as u64),
        unpacked_size,
        unpack_version,
        method,
        crc16,
    })
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
        return decode_rar4_plain_name(data);
    };

    let ascii_name = &data[..null_pos];
    let unicode_data = &data[null_pos + 1..];

    if unicode_data.is_empty() {
        return decode_rar4_plain_name(ascii_name);
    }

    let mut enc_pos = 0usize;
    let mut dec_pos = 0usize;
    let high_byte = unicode_data.get(enc_pos).copied().unwrap_or(0);
    enc_pos = enc_pos.saturating_add(1);

    let mut flags = 0u8;
    let mut flag_bits = 0u8;
    let mut result = Vec::with_capacity(ascii_name.len());

    while enc_pos < unicode_data.len() {
        if flag_bits == 0 {
            flags = unicode_data[enc_pos];
            enc_pos += 1;
            flag_bits = 8;
            if enc_pos > unicode_data.len() {
                break;
            }
        }

        match flags >> 6 {
            0 => {
                if enc_pos >= unicode_data.len() {
                    break;
                }
                result.resize(dec_pos + 1, 0);
                result[dec_pos] = unicode_data[enc_pos] as u16;
                enc_pos += 1;
                dec_pos += 1;
            }
            1 => {
                if enc_pos >= unicode_data.len() {
                    break;
                }
                result.resize(dec_pos + 1, 0);
                result[dec_pos] = unicode_data[enc_pos] as u16 | ((high_byte as u16) << 8);
                enc_pos += 1;
                dec_pos += 1;
            }
            2 => {
                if enc_pos + 1 >= unicode_data.len() {
                    break;
                }
                result.resize(dec_pos + 1, 0);
                result[dec_pos] =
                    unicode_data[enc_pos] as u16 | ((unicode_data[enc_pos + 1] as u16) << 8);
                enc_pos += 2;
                dec_pos += 1;
            }
            3 => {
                if enc_pos >= unicode_data.len() {
                    break;
                }
                let mut length = unicode_data[enc_pos];
                enc_pos += 1;

                if (length & 0x80) != 0 {
                    if enc_pos >= unicode_data.len() {
                        break;
                    }
                    let correction = unicode_data[enc_pos];
                    enc_pos += 1;
                    for _ in 0..(((length & 0x7F) as usize) + 2) {
                        if dec_pos >= ascii_name.len() {
                            break;
                        }
                        result.resize(dec_pos + 1, 0);
                        result[dec_pos] = (((ascii_name[dec_pos] as u16)
                            .wrapping_add(correction as u16))
                            & 0x00FF)
                            | ((high_byte as u16) << 8);
                        dec_pos += 1;
                    }
                } else {
                    length = length.saturating_add(2);
                    for _ in 0..length {
                        if dec_pos >= ascii_name.len() {
                            break;
                        }
                        result.resize(dec_pos + 1, 0);
                        result[dec_pos] = ascii_name[dec_pos] as u16;
                        dec_pos += 1;
                    }
                }
            }
            _ => unreachable!(),
        }

        flags <<= 2;
        flag_bits -= 2;
    }

    if result.is_empty() {
        return decode_rar4_plain_name(ascii_name);
    }

    if let Some(zero_pos) = result.iter().position(|unit| *unit == 0) {
        result.truncate(zero_pos);
    }

    String::from_utf16_lossy(&result)
}

fn decode_rar4_c_string_name(data: &[u8]) -> String {
    let nul_end = data
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(data.len());
    decode_rar4_plain_name(&data[..nul_end])
}

fn decode_rar4_plain_name(data: &[u8]) -> String {
    crate::header::common::decode_utf8_prefix_until_nul(data)
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

    let expected_crc = read_u16(&data, 0);
    let actual_crc = rar4_header_crc16(&data);
    if actual_crc != expected_crc {
        return Err(RarError::InvalidPassword);
    }

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
        let crc16 = rar4_header_crc16(&buf);
        buf[0..2].copy_from_slice(&crc16.to_le_bytes());
        buf
    }

    fn dos_dt(year: u32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> u32 {
        let time = ((hour as u16) << 11) | ((minute as u16) << 5) | ((second as u16) / 2);
        let date = (((year - 1980) as u16) << 9) | ((month as u16) << 5) | day as u16;
        ((date as u32) << 16) | u32::from(time)
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
        let mut main_fields = Vec::new();
        main_fields.extend_from_slice(&0x1234u16.to_le_bytes());
        main_fields.extend_from_slice(&0x5678_9abcu32.to_le_bytes());
        let data = build_raw_header(
            0x73,
            archive_flags::SOLID | archive_flags::VOLUME | archive_flags::COMMENT,
            &main_fields,
        );
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let arch = parse_archive_header(&raw).unwrap();
        assert!(arch.is_solid);
        assert!(arch.is_volume);
        assert!(!arch.is_encrypted);
        assert!(arch.comment_in_header);
        assert_eq!(arch.high_pos_av, 0x1234);
        assert_eq!(arch.pos_av, 0x5678_9abc);
        assert!(arch.is_signed);
    }

    #[test]
    fn test_parse_rar14_raw_main_comment() {
        let mut body = Vec::new();
        body.extend_from_slice(&5u16.to_le_bytes());
        body.extend_from_slice(b"hello");

        let comment = parse_rar14_main_comment(&body, 0x100, archive_flags::COMMENT)
            .unwrap()
            .unwrap();

        assert_eq!(comment.header_offset, 0x100);
        assert_eq!(comment.data_offset, 0x100 + 9);
        assert_eq!(comment.packed_size, 5);
        assert_eq!(comment.unpacked_size, 5);
        assert_eq!(comment.unpack_version, 0);
        assert_eq!(comment.method, Rar4Method::Store);
    }

    #[test]
    fn test_parse_rar14_packed_main_comment() {
        let mut body = Vec::new();
        body.extend_from_slice(&6u16.to_le_bytes());
        body.extend_from_slice(&12u16.to_le_bytes());
        body.extend_from_slice(&[1, 2, 3, 4]);

        let comment = parse_rar14_main_comment(
            &body,
            0x200,
            archive_flags::COMMENT | archive_flags::PACK_COMMENT,
        )
        .unwrap()
        .unwrap();

        assert_eq!(comment.header_offset, 0x200);
        assert_eq!(comment.data_offset, 0x200 + 11);
        assert_eq!(comment.packed_size, 4);
        assert_eq!(comment.unpacked_size, 12);
        assert_eq!(comment.unpack_version, 15);
        assert_eq!(comment.method, Rar4Method::Normal);
    }

    #[test]
    fn test_parse_recovery_header_matches_unrar_protect_head() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&4096u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&12u16.to_le_bytes());
        extra.extend_from_slice(&512u32.to_le_bytes());
        extra.extend_from_slice(b"Protect!");

        let data = build_raw_header(0x78, common_flags::HAS_DATA, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let rr = parse_recovery_header(&raw).unwrap();

        assert_eq!(rr.header_offset, 0);
        assert_eq!(rr.data_offset, raw.header_size as u64);
        assert_eq!(rr.data_size, 4096);
        assert_eq!(rr.version, 3);
        assert_eq!(rr.recovery_sectors, 12);
        assert_eq!(rr.total_blocks, 512);
        assert_eq!(&rr.mark, b"Protect!");
    }

    #[test]
    fn test_parse_recovery_header_truncated_marker_is_lossy_like_unrar() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&4096u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&12u16.to_le_bytes());
        extra.extend_from_slice(&512u32.to_le_bytes());
        extra.extend_from_slice(b"Pro");

        let data = build_raw_header(0x78, common_flags::HAS_DATA, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let rr = parse_recovery_header(&raw).unwrap();

        assert_eq!(rr.data_offset, raw.header_size as u64);
        assert_eq!(rr.data_size, 4096);
        assert_eq!(rr.version, 3);
        assert_eq!(rr.recovery_sectors, 12);
        assert_eq!(rr.total_blocks, 512);
        assert_eq!(&rr.mark, b"Pro\0\0\0\0\0");
    }

    #[test]
    fn test_parse_old_service_ntacl_like_unrar() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&1234u32.to_le_bytes());
        extra.extend_from_slice(&OLD_SERVICE_NTACL.to_le_bytes());
        extra.push(2);
        extra.extend_from_slice(&4096u32.to_le_bytes());
        extra.push(20);
        extra.push(0x33);
        extra.extend_from_slice(&0xAABB_CCDDu32.to_le_bytes());

        let data = build_raw_header(0x77, common_flags::HAS_DATA, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let old = parse_old_service_header(&raw).unwrap();

        assert_eq!(old.data_size, 1234);
        assert_eq!(old.subtype, OLD_SERVICE_NTACL);
        assert_eq!(old.level, 2);
        assert_eq!(
            old.data,
            Rar4OldServiceData::NtAcl {
                unpacked_size: 4096,
                unpack_version: 20,
                method: Rar4Method::Normal,
                crc32: 0xAABB_CCDD,
            }
        );
    }

    #[test]
    fn test_parse_old_service_truncated_base_fields_are_lossy_like_unrar() {
        let extra = 1234u32.to_le_bytes();
        let data = build_raw_header(0x77, common_flags::HAS_DATA, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let old = parse_old_service_header(&raw).unwrap();

        assert_eq!(old.data_offset, raw.header_size as u64);
        assert_eq!(old.data_size, 1234);
        assert_eq!(old.subtype, 0);
        assert_eq!(old.level, 0);
        assert_eq!(old.data, Rar4OldServiceData::Unknown);
    }

    #[test]
    fn test_parse_old_service_stream_name_is_capped_like_unrar() {
        let long_name = vec![b'a'; MAX_STREAM_NAME20 + 10];
        let old = parse_test_old_service_stream(&long_name);

        match old.data {
            Rar4OldServiceData::Stream {
                unpacked_size,
                unpack_version,
                method,
                crc32,
                stream_name,
            } => {
                assert_eq!(unpacked_size, 99);
                assert_eq!(unpack_version, 20);
                assert_eq!(method, Rar4Method::Store);
                assert_eq!(crc32, 0x1122_3344);
                assert_eq!(stream_name.len(), MAX_STREAM_NAME20);
                assert!(stream_name.bytes().all(|byte| byte == b'a'));
            }
            other => panic!("expected Stream old service, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_old_service_stream_name_overlong_utf8_decodes_like_unrar() {
        let old = parse_test_old_service_stream(b":safe\xc0\xafstream");

        match old.data {
            Rar4OldServiceData::Stream { stream_name, .. } => {
                assert_eq!(stream_name, ":safe/stream");
            }
            other => panic!("expected Stream old service, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_old_service_stream_name_invalid_utf8_keeps_prefix_like_unrar() {
        let old = parse_test_old_service_stream(b":safe/\xffhidden");

        match old.data {
            Rar4OldServiceData::Stream { stream_name, .. } => {
                assert_eq!(stream_name, ":safe/");
            }
            other => panic!("expected Stream old service, got {other:?}"),
        }
    }

    fn parse_test_old_service_stream(name: &[u8]) -> Rar4OldServiceHeader {
        let mut extra = Vec::new();
        extra.extend_from_slice(&55u32.to_le_bytes());
        extra.extend_from_slice(&OLD_SERVICE_STREAM.to_le_bytes());
        extra.push(1);
        extra.extend_from_slice(&99u32.to_le_bytes());
        extra.push(20);
        extra.push(0x30);
        extra.extend_from_slice(&0x1122_3344u32.to_le_bytes());
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x77, common_flags::HAS_DATA, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        parse_old_service_header(&raw).unwrap()
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
        assert_eq!(fh.unpacked_size, Some(200));
        assert_eq!(fh.host_os, Rar4HostOs::Unix);
        assert_eq!(fh.crc32, 0xDEADBEEF);
        assert_eq!(fh.method, Rar4Method::Normal);
        assert_eq!(fh.unpack_version, 29);
        assert_eq!(fh.dict_size, 0x1_0000);
        assert!(!fh.is_encrypted);
        assert!(!fh.is_directory);
    }

    #[test]
    fn test_parse_file_header_truncated_fixed_fields_are_lossy_like_unrar() {
        let data = build_raw_header(0x74, 0, &[]);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "");
        assert_eq!(fh.packed_size, 0);
        assert_eq!(fh.unpacked_size, Some(0));
        assert_eq!(fh.host_os, Rar4HostOs::MsDos);
        assert_eq!(fh.crc32, 0);
        assert_eq!(fh.mtime, 0);
        assert_eq!(fh.unpack_version, 0);
        assert_eq!(fh.method, Rar4Method::Unknown(0));
        assert_eq!(fh.attributes, 0);
    }

    #[test]
    fn test_parse_file_header_truncated_name_is_zero_padded_like_unrar() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes()); // packed size
        extra.extend_from_slice(&0u32.to_le_bytes()); // unpacked size
        extra.push(3); // Unix
        extra.extend_from_slice(&0u32.to_le_bytes()); // CRC32
        extra.extend_from_slice(&0u32.to_le_bytes()); // datetime
        extra.push(29);
        extra.push(0x30); // Store
        extra.extend_from_slice(&8u16.to_le_bytes()); // name length
        extra.extend_from_slice(&0u32.to_le_bytes()); // attributes
        extra.extend_from_slice(b"abc");

        let data = build_raw_header(0x74, 0, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "abc");
    }

    #[test]
    fn test_parse_file_header_comment_flag_matches_unrar() {
        let name = b"commented.txt";
        let mut extra = Vec::new();
        extra.extend_from_slice(&10u32.to_le_bytes());
        extra.extend_from_slice(&20u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, file_flags::COMMENT, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert!(fh.comment_in_header);
    }

    #[test]
    fn test_parse_file_header_non_unicode_name_truncates_at_nul_like_unrar() {
        let name = b"safe.txt\0hidden.txt";
        let mut extra = Vec::new();
        extra.extend_from_slice(&10u32.to_le_bytes());
        extra.extend_from_slice(&20u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, 0, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "safe.txt");
    }

    #[test]
    fn test_parse_service_header_non_unicode_name_truncates_at_nul_like_unrar() {
        let name = b"UOW\0ignored";
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x7a, 0, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "UOW");
    }

    #[test]
    fn test_parse_service_header_solid_flag_means_subblock_like_unrar() {
        let name = b"CMT";
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x7a, file_flags::SOLID, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert!(!fh.is_solid);
        assert!(fh.is_subblock);
    }

    #[test]
    fn test_parse_service_header_truncated_salt_is_padded_like_unrar() {
        let name = b"UOW";
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);
        extra.extend_from_slice(&[4, 5, 6]);

        let data = build_raw_header(0x7a, file_flags::SALT, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "UOW");
        assert_eq!(fh.service_subdata, None);
        assert_eq!(fh.salt, Some([4, 5, 6, 0, 0, 0, 0, 0]));
    }

    #[test]
    fn test_parse_file_header_extended_times_like_unrar() {
        let name = b"times.txt";
        let base_mtime = dos_dt(2024, 5, 6, 1, 2, 4);
        let ctime = dos_dt(2024, 5, 7, 3, 4, 6);
        let atime = dos_dt(2024, 5, 8, 5, 6, 8);
        let mut extra = Vec::new();
        extra.extend_from_slice(&10u32.to_le_bytes()); // packed size
        extra.extend_from_slice(&20u32.to_le_bytes()); // unpacked size
        extra.push(3); // Unix
        extra.extend_from_slice(&0u32.to_le_bytes()); // CRC32
        extra.extend_from_slice(&base_mtime.to_le_bytes());
        extra.push(29);
        extra.push(0x30); // Store
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes()); // attributes
        extra.extend_from_slice(name);
        let flags = ((0x08 | 0x04 | 0x03) << 12) | (0x08 << 8) | ((0x08 | 0x01) << 4);
        extra.extend_from_slice(&(flags as u16).to_le_bytes());
        extra.extend_from_slice(&[1, 2, 3]); // mtime 100ns precision bytes.
        extra.extend_from_slice(&ctime.to_le_bytes());
        extra.extend_from_slice(&atime.to_le_bytes());
        extra.push(5); // atime precision byte.

        let data = build_raw_header(0x74, file_flags::EXT_TIME, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        let base = dos_datetime_to_system_time(base_mtime).unwrap();
        let expected_mtime = base + Duration::from_secs(1) + Duration::from_nanos(0x03_02_01 * 100);
        let expected_atime =
            dos_datetime_to_system_time(atime).unwrap() + Duration::from_nanos((5u64 << 16) * 100);
        assert_eq!(fh.mtime_precise, Some(expected_mtime));
        assert_eq!(fh.ctime, dos_datetime_to_system_time(ctime));
        assert_eq!(fh.atime, Some(expected_atime));
    }

    #[test]
    fn test_parse_file_header_version_suffix_like_unrar() {
        let name = b"report.txt;42beta";
        let mut extra = Vec::new();
        extra.extend_from_slice(&10u32.to_le_bytes());
        extra.extend_from_slice(&20u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, file_flags::VERSION, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "report.txt;42beta");
        assert_eq!(fh.version, Some(42));
    }

    #[test]
    fn test_parse_file_header_ignores_version_suffix_without_flag() {
        let name = b"report.txt;42";
        let mut extra = Vec::new();
        extra.extend_from_slice(&10u32.to_le_bytes());
        extra.extend_from_slice(&20u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, 0, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.name, "report.txt;42");
        assert_eq!(fh.version, None);
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
        assert_eq!(fh.unpacked_size, Some(0x2_FFFF_FFFF));
    }

    #[test]
    fn test_parse_file_header_truncated_large_fields_are_lossy_like_unrar() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&0x1122_3344u32.to_le_bytes()); // packed low
        extra.extend_from_slice(&0x5566_7788u32.to_le_bytes()); // unpacked low
        extra.push(2); // Windows
        extra.extend_from_slice(&0u32.to_le_bytes()); // CRC
        extra.extend_from_slice(&0u32.to_le_bytes()); // datetime
        extra.push(29);
        extra.push(0x30); // Store
        extra.extend_from_slice(&0u16.to_le_bytes()); // empty name
        extra.extend_from_slice(&0u32.to_le_bytes()); // attrs
        extra.extend_from_slice(&1u32.to_le_bytes()); // packed high; unpacked high missing

        let data = build_raw_header(0x74, file_flags::LARGE, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.packed_size, 0x1_1122_3344);
        assert_eq!(fh.unpacked_size, Some(0x5566_7788));
    }

    #[test]
    fn test_parse_file_header_truncated_salt_is_padded_like_unrar() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes()); // packed low
        extra.extend_from_slice(&0u32.to_le_bytes()); // unpacked low
        extra.push(2); // Windows
        extra.extend_from_slice(&0u32.to_le_bytes()); // CRC
        extra.extend_from_slice(&0u32.to_le_bytes()); // datetime
        extra.push(29);
        extra.push(0x30); // Store
        extra.extend_from_slice(&0u16.to_le_bytes()); // empty name
        extra.extend_from_slice(&0u32.to_le_bytes()); // attrs
        extra.extend_from_slice(&[1, 2, 3]); // short salt

        let data = build_raw_header(0x74, file_flags::SALT, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.salt, Some([1, 2, 3, 0, 0, 0, 0, 0]));
    }

    #[test]
    fn test_parse_file_header_unknown_unpacked_size_without_large() {
        let name = b"stream.bin";
        let mut extra = Vec::new();
        extra.extend_from_slice(&123u32.to_le_bytes());
        extra.extend_from_slice(&u32::MAX.to_le_bytes());
        extra.push(2);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, 0, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert_eq!(fh.unpacked_size, None);
    }

    #[test]
    fn test_parse_file_header_tracks_rar4_encryption_method_without_salt() {
        let name = b"enc.bin";
        let mut extra = Vec::new();
        extra.extend_from_slice(&64u32.to_le_bytes());
        extra.extend_from_slice(&64u32.to_le_bytes());
        extra.push(3);
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(20);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, file_flags::ENCRYPTED, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert!(fh.is_encrypted);
        assert_eq!(fh.salt, None);
        assert_eq!(fh.encryption_method, Some(Rar4EncryptionMethod::Rar20));
    }

    #[test]
    fn test_parse_file_header_detects_unix_symlink() {
        let name = b"link";
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(3); // Unix
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0xA000u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, 0, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert!(fh.is_unix_symlink);
    }

    #[test]
    fn test_parse_file_header_unix_regular_mode_is_not_directory() {
        let name = b"video.mkv";
        let mut extra = Vec::new();
        extra.extend_from_slice(&100u32.to_le_bytes());
        extra.extend_from_slice(&200u32.to_le_bytes());
        extra.push(3); // Unix
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0o100755u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(
            0x74,
            file_flags::SPLIT_BEFORE | file_flags::SPLIT_AFTER,
            &extra,
        );
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert!(!fh.is_directory);
    }

    #[test]
    fn test_parse_file_header_zero_byte_windows_directory() {
        let name = b"extras";
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(2); // Windows
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(29);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0x10u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, file_flags::DIRECTORY, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert!(fh.is_directory);
    }

    #[test]
    fn test_parse_file_header_legacy_attr_directory() {
        let name = b"extras";
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(2); // Windows
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.extend_from_slice(&0u32.to_le_bytes());
        extra.push(15);
        extra.push(0x30);
        extra.extend_from_slice(&(name.len() as u16).to_le_bytes());
        extra.extend_from_slice(&0x10u32.to_le_bytes());
        extra.extend_from_slice(name);

        let data = build_raw_header(0x74, 0, &extra);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let fh = parse_file_header(&raw).unwrap();

        assert!(fh.is_directory);
    }

    #[test]
    fn test_unicode_name_no_null() {
        let name = b"simple.txt";
        let result = decode_rar4_unicode_name(name);
        assert_eq!(result, "simple.txt");
    }

    #[test]
    fn test_plain_name_invalid_utf8_keeps_prefix_like_unrar() {
        let result = decode_rar4_unicode_name(b"safe/\xffhidden");
        assert_eq!(result, "safe/");
    }

    #[test]
    fn test_plain_name_overlong_utf8_decodes_like_unrar() {
        let result = decode_rar4_unicode_name(b"safe\xc0\xafname.txt");
        assert_eq!(result, "safe/name.txt");
    }

    #[test]
    fn test_unicode_name_with_null_no_unicode() {
        let mut data = b"name.txt".to_vec();
        data.push(0);
        let result = decode_rar4_unicode_name(&data);
        assert_eq!(result, "name.txt");
    }

    #[test]
    fn test_unicode_name_high_byte_only_falls_back_to_ascii() {
        let data = [b'n', b'a', b'm', b'e', 0, 0x01];
        let result = decode_rar4_unicode_name(&data);
        assert_eq!(result, "name");
    }

    #[test]
    fn test_unicode_name_truncated_stream_does_not_append_ascii_tail() {
        // UnRAR's EncodeFileName::Decode stops at the decoded Unicode length;
        // it does not append undecoded bytes from the ASCII fallback name.
        let data = [b'a', b'b', b'c', b'd', 0, 0x00, 0x00, b'x'];
        let result = decode_rar4_unicode_name(&data);
        assert_eq!(result, "x");
    }

    #[test]
    fn test_unicode_name_embedded_nul_truncates_like_unrar() {
        // ConvertFileHeader truncates broken decoded names at the first NUL.
        let data = [b'a', b'b', 0, 0x00, 0x00, 0, b'z'];
        let result = decode_rar4_unicode_name(&data);
        assert_eq!(result, "");
    }

    #[test]
    fn test_unicode_name_mode3_correction_matches_unrar_layout() {
        let data = [b'a', b'b', 0, 0x01, 0xC0, 0x80, 0x00];
        let result = decode_rar4_unicode_name(&data);
        let units: Vec<u16> = result.encode_utf16().collect();
        assert_eq!(units, vec![0x0161, 0x0162]);
    }

    #[test]
    fn test_comment_header_optional_fields_match_unrar_order() {
        let mut body = Vec::new();
        body.extend_from_slice(&4u16.to_le_bytes());
        body.push(29);
        body.push(0x30);
        body.extend_from_slice(&0xBEEFu16.to_le_bytes());
        body.extend_from_slice(b"note");
        let data = build_raw_header(0x75, 0, &body);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let comment = parse_comment_header(&raw).unwrap();

        assert_eq!(comment.header_offset, 0);
        assert_eq!(comment.data_offset, 13);
        assert_eq!(comment.packed_size, 4);
        assert_eq!(comment.unpacked_size, 4);
        assert_eq!(comment.unpack_version, 29);
        assert_eq!(comment.method, Rar4Method::Store);
        assert_eq!(comment.crc16, 0xBEEF);
    }

    #[test]
    fn test_comment_header_truncated_fields_are_lossy_like_unrar() {
        let body = 7u16.to_le_bytes();
        let data = build_raw_header(0x75, 0, &body);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let comment = parse_comment_header(&raw).unwrap();

        assert_eq!(comment.data_offset, 9);
        assert_eq!(comment.packed_size, 0);
        assert_eq!(comment.unpacked_size, 7);
        assert_eq!(comment.unpack_version, 0);
        assert_eq!(comment.method, Rar4Method::Unknown(0));
        assert_eq!(comment.crc16, 0);
    }

    #[test]
    fn test_end_header() {
        let data = build_raw_header(0x7B, 0x0001, &[]);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let end = parse_end_header(&raw);
        assert!(end.more_volumes);
        assert!(!end.has_data_crc);
        assert!(!end.rev_space);
        assert_eq!(end.data_crc, None);
        assert_eq!(end.volume_number, None);
    }

    #[test]
    fn test_end_header_optional_fields_match_unrar_order() {
        let mut body = Vec::new();
        body.extend_from_slice(&0xAABB_CCDDu32.to_le_bytes());
        body.extend_from_slice(&7u16.to_le_bytes());
        let flags = end_flags::NEXT_VOLUME
            | end_flags::DATA_CRC
            | end_flags::REV_SPACE
            | end_flags::VOLUME_NUMBER;
        let data = build_raw_header(0x7B, flags, &body);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let end = parse_end_header(&raw);

        assert_eq!(end.flags, flags);
        assert!(end.more_volumes);
        assert!(end.has_data_crc);
        assert!(end.rev_space);
        assert_eq!(end.data_crc, Some(0xAABB_CCDD));
        assert_eq!(end.volume_number, Some(7));
    }

    #[test]
    fn test_end_header_truncated_optional_fields_are_lossy_like_unrar() {
        let body = 9u16.to_le_bytes();
        let flags = end_flags::DATA_CRC | end_flags::VOLUME_NUMBER;
        let data = build_raw_header(0x7B, flags, &body);
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let end = parse_end_header(&raw);

        assert!(end.has_data_crc);
        assert_eq!(end.data_crc, Some(0));
        assert_eq!(end.volume_number, Some(9));
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

    #[test]
    fn test_header_crc_mismatch_warns_but_parses_like_unrar() {
        let mut data = build_raw_header(0x73, 0x0000, &[0x00; 6]);
        data[0] ^= 0x01;
        let mut cursor = Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        assert_eq!(raw.header_type, Rar4HeaderType::Archive);
        assert_eq!(raw.flags, 0);
    }
}
